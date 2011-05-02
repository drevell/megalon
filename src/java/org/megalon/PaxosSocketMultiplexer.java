package org.megalon;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config.Host;
import org.megalon.avro.AvroAccept;
import org.megalon.avro.AvroAcceptResponse;
import org.megalon.avro.AvroPrepare;
import org.megalon.avro.AvroPrepareResponse;
import org.megalon.avro.AvroWALEntry;
import org.megalon.messages.MegalonMsg;

/**
 * Normal Avro RPC would have us send a request over a socket, wait for a
 * response, then possibly reuse the socket for another request. This sucks
 * because it requires several sockets to process several requests concurrently.
 * Instead we want to have many operations in flight at the same time using only
 * one socket, which is why this class exists.
 * 
 * The basic idea is that each request results in a message being sent over the
 * socket with a unique serial number. When/if the remote server wants to send a
 * response to a request, it sends a response packet with the same serial 
 * number as the request. The requester can match the response to the request.
 * 
 * TODO more documentation, about waiters?
 */
public class PaxosSocketMultiplexer {
	SocketChannel schan;
	long reqSerial = 0;
	Host host;
	String replica;
	@SuppressWarnings("rawtypes")
	Map<Long, RespWaiter> waiters = new ConcurrentHashMap<Long, RespWaiter>();
	Log logger = LogFactory.getLog(PaxosSocketMultiplexer.class);
	Thread reader;
	long lastReconnectAttemptTime = 0;
	public static final long reconnectAttemptIntervalMs = 5000;
	
	// TODO weak refs to waiters, caller may have given up
	// TODO non-blocking
	// TODO shouldn't re-encode for each host
	
	final DatumWriter<AvroPrepare> prepareWriter = 
		new SpecificDatumWriter<AvroPrepare>(AvroPrepare.class); 
	final DatumWriter<AvroAccept> acceptWriter = 
		new SpecificDatumWriter<AvroAccept>(AvroAccept.class); 
	
	public PaxosSocketMultiplexer(Host host, String replica) {
		this.host = host;
		this.replica = replica;
		reconnect();
		reader = new Thread() {
			public void run() {
				while(true) {
					socketReadAndHandle();
				}
			}
		};
		reader.setDaemon(true);
		reader.start();
	}
	
	protected synchronized boolean reconnect() {
		long nowTimeMs = System.currentTimeMillis();
		if(lastReconnectAttemptTime + reconnectAttemptIntervalMs < nowTimeMs) {
			lastReconnectAttemptTime = nowTimeMs; 
			try {
				this.schan = SocketChannel.open(new InetSocketAddress(
						host.nameOrAddr, host.port));
				logger.debug("Multiplexer connected to " + host);
				return true;
			} catch (IOException e) {
				logger.info("Multiplexer connection to " + host + 
						" had exception", e);
			}
		}
		return false;
	}
	
	/**
	 * This is the main function for a thread that sits on a socket connected
	 * to a Replication Server. When the remote server sends a response to an
	 * RPC call, this function deserializes the result and passes it to any
	 * RespWaiter that might be waiting for the response.
	 */
	protected void socketReadAndHandle() {
		boolean doReconnect = false;
		if(schan == null || !schan.isConnected()) {
			doReconnect = true;
		} else {
			try {
				logger.debug("socketReadAndHandle: RPC response listener " +
						"running for remote host " + host);
//				InputStream is = schan.socket().getInputStream();
				BinaryDecoder dec = null;
				
				DatumReader<AvroPrepareResponse> prepareRespReader = new 
					SpecificDatumReader<AvroPrepareResponse>(AvroPrepareResponse.class);
				DatumReader<AvroAcceptResponse> acceptRespReader = new 
					SpecificDatumReader<AvroAcceptResponse>(AvroAcceptResponse.class);
			
				AvroPrepareResponse prepareResp = new AvroPrepareResponse();
				AvroAcceptResponse acceptResp = new AvroAcceptResponse();
				ByteBuffer bufLenBuf = ByteBuffer.allocate(4);
				while(true) {
					bufLenBuf.clear();
					if(schan.read(bufLenBuf) == 0) {
						continue;
					}
					int bufLen = Util.bytesToInt(bufLenBuf.array());
					// TODO The following allocation prob. causes GC pressure
					ByteBuffer msgBuf = ByteBuffer.allocate(bufLen);
					insistRead(msgBuf, bufLen);
					logger.debug("Incoming bufLen: " + bufLen);
					
					ByteArrayInputStream bis = 
						new ByteArrayInputStream(msgBuf.array());
					dec = DecoderFactory.get().binaryDecoder(bis, dec);
					
					long reqSerial;
					Object responseObj = null;
					int msgType = bis.read();
					logger.debug("Got RPC response msg " + msgType);
					switch(msgType) {
					case MegalonMsg.MSG_PREPARE_RESP:
						prepareResp = prepareRespReader.read(prepareResp, dec);
						reqSerial = prepareResp.reqSerial;
						if(prepareResp.walEntry != null) {
							responseObj = new WALEntry(prepareResp.walEntry);
						}
						break;
					case MegalonMsg.MSG_ACCEPT_RESP:
						acceptResp = acceptRespReader.read(acceptResp, dec);
						reqSerial = acceptResp.reqSerial;
						responseObj = acceptResp.acked;
						break;
					default:
						logger.warn("RPC response was an unrecognized msg type");
						throw new IOException();
					}
	
					RespWaiter waiter = waiters.get(reqSerial);
					// waiter may be a soft reference to a GC'ed object
					if(waiter != null) {
						waiter.response(replica, responseObj);
					}
				}
			} catch (Exception e) {
				doReconnect = true;
				try {
					schan.close();
				} catch (IOException ex) {}
				logger.warn("Exception in RPC socket read, reconnecting", e);
			}
		}
		
		if(doReconnect) {
			if(!reconnect()) {
				try {
					Thread.sleep(100); // TODO should backoff
				} catch (InterruptedException ex) {}
			}
		}
	}
	
	/**
	 * Read len bytes into the buffers, throwing an IOException if the read
	 * returns prematurely.
	 */
	void insistRead(ByteBuffer buf, int len) throws IOException {
		// TODO does this work? is it common to get fewer bytes than requested?
		if(schan.read(buf) != len) {
			throw new IOException("Unexpected short read");
		}
	}
	
	protected synchronized long getNextSerial() {
		reqSerial++;
		return reqSerial;
	}
	
	/**
	 * If there has been a socket error, this call could fail. If this happens,
	 * the caller should create another object and try again. (TODO correct?)
	 */
	public void prepare(long walIndex, long n, RespWaiter<WALEntry> waiter)
	throws IOException {
//		AvroPrepare msg = new AvroPrepare();
////		msg.reqSerial = getNextSerial();
//		msg.n = n;
//		msg.walIndex = walIndex;


		// TODO pool these for less GC?
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		// TODO cache encoders? they're not thread safe though
		BinaryEncoder e = EncoderFactory.get().binaryEncoder(bao, null);
		bao.write(MegalonMsg.MSG_PREPARE);
		prepareWriter.write(msg, e);
		e.flush();

		waiters.put(msg.reqSerial, waiter); // Waiter will run if/when response
		
		writeLengthPrefixed(bao.toByteArray());
		logger.debug("wrote prepare msg to replica: " + replica + " host: " + 
				host);		
	}
	
	/**
	 * If there has been a socket error, this call could fail. If this happens,
	 * the caller should create another object and try again. (TODO correct?)
	 */
	public void accept(long walIndex, AvroWALEntry entry, 
			RespWaiter<Object> waiter) throws IOException {
		AvroAccept msg = new AvroAccept();
		msg.walEntry = entry;
		msg.reqSerial = getNextSerial();
		msg.walIndex = walIndex;
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		// TODO cache encoders? they're not thread safe though
		BinaryEncoder e = EncoderFactory.get().binaryEncoder(bao, null);
		bao.write(MegalonMsg.MSG_ACCEPT);
		acceptWriter.write(msg, e);
		e.flush();
		byte[] outgoingBytes = bao.toByteArray();
		writeLengthPrefixed(outgoingBytes);
		logger.debug("Wrote accept msg to replica: " + replica + " host: " + 
				host);
	}
	
	/**
	 * Given a byte array, write to the socket: 4 bytes of array length, then
	 * the full array.
	 */
	void writeLengthPrefixed(byte[] bytes) throws IOException {
		byte[] lengthBuf = Util.intToBytes(bytes.length);
		ByteBuffer[] bufsToWrite = new ByteBuffer[] {
				ByteBuffer.wrap(lengthBuf),
				ByteBuffer.wrap(bytes)};
		long nBytesWritten = schan.write(bufsToWrite);

		logger.debug("RPC socket request sent nbytes: " + nBytesWritten);
		logger.debug("Output length field: " + Arrays.toString(lengthBuf));
		logger.debug("Output data field: " + Arrays.toString(bytes));
		if(nBytesWritten != bytes.length + 4) {
			throw new AssertionError();
		}
	}
	
	static class RespWaiter<T> {
		public Map<String,T> responses;
		int validResponses = 0;
		int nackResponses = 0;
		int numExpected;
		Log logger = LogFactory.getLog(RespWaiter.class);
		
		public RespWaiter(int numExpected) {
			responses = new HashMap<String,T>();
			this.numExpected = numExpected;
		}
		
		synchronized public void response(String replica, T response) {
			responses.put(replica, response);
			validResponses++;
			notify();
		}
		
		synchronized public void nack(String replica) {
			responses.put(replica, null);
			nackResponses++;
			notify();
		}
		
		synchronized public boolean waitForQuorum(int timeoutMs) {
			long startTime = System.currentTimeMillis();
			while(true) {
				if(validResponses >= numExpected / 2 + 1) {
					// Quorum has been reached
					logger.debug("quorum reached");
					notifyAll();
					return true;
				}
				if(nackResponses >= (numExpected + 1) / 2) {
					// No quorum is possible, half of responses are nacks
					logger.debug("waitForQuorum impossible quorum");
					notifyAll();
					return false;
				}
				long waitMs = timeoutMs - (System.currentTimeMillis() - 
						startTime);
				if(waitMs > 0) {
					try {
						wait(waitMs);
					} catch (InterruptedException e) {}
				} else {
					logger.debug("waitForQuorum timeout");
					notifyAll();
					return false;
				}
			}
		}
		
		synchronized Collection<T> getResponses() {
			return responses.values();
		}
	}
}
