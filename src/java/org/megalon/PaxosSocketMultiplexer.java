package org.megalon;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config.Host;
import org.megalon.avro.AvroAccept;
import org.megalon.avro.AvroAcceptResponse;
import org.megalon.avro.AvroPrepare;
import org.megalon.avro.AvroPrepareResponse;
import org.megalon.avro.AvroWALEntry;

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
	Map<Long, RespWaiter> prepWaiters = new HashMap<Long, RespWaiter>();
	Log logger = LogFactory.getLog(PaxosSocketMultiplexer.class);
	Thread reader;
	
	public static final byte MSG_PREPARE = 1;
	public static final byte MSG_PREPARE_RESP = 2;
	public static final byte MSG_ACCEPT = 3;
	public static final byte MSG_ACCEPT_RESP = 4;
	
	// TODO weak refs
	// TODO non-blocking
	
	final DatumWriter<AvroPrepare> prepareWriter = 
		new SpecificDatumWriter<AvroPrepare>(AvroPrepare.class); 
	final DatumWriter<AvroAccept> acceptWriter = 
		new SpecificDatumWriter<AvroAccept>(AvroAccept.class); 
	
	public PaxosSocketMultiplexer(Host host) throws IOException {
		this.host = host;
		schan = SocketChannel.open(new InetSocketAddress(host.nameOrAddr, 
				host.port));
		reader = new Thread() {
			public void run() {
				socketReadLoop();
			}
		};
		reader.start();
	}
	
	protected void socketReadLoop() {
		try {
			InputStream is = schan.socket().getInputStream();
			while(true) {
				byte[] msgType = new byte[1];
				is.read(msgType, 0, 1);
				switch(msgType[0]) {
				case MSG_PREPARE_RESP:
					// TODO avro response decode and send to waiter
					break;
				case MSG_ACCEPT_RESP:
					// TODO avro response decode and send to waiter
					break;
				default:
					logger.warn("RPC response was an unrecognized msg type");
					throw new IOException();
				}
			}
		} catch (IOException e) {
			logger.warn("IOException in RPC socket read loop");
			try {
				schan.close();
			} catch (IOException ex) {}
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
	void prepare(long walIndex, long n, 
			RespWaiter<AvroPrepareResponse> waiter) {
		AvroPrepare msg = new AvroPrepare();
		msg.reqSerial = getNextSerial();
		msg.n = n;
		msg.walIndex = walIndex;
		// TODO pool these for less GC?
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		BinaryEncoder e = EncoderFactory.get().binaryEncoder(bao, null);
		try {
			bao.write(MSG_PREPARE);
			prepareWriter.write(msg, e);
			e.flush();
			byte[] outgoingBytes = bao.toByteArray();
			prepWaiters.put(msg.reqSerial, waiter);
			schan.write(ByteBuffer.wrap(outgoingBytes));
		} catch (IOException ex) {
			throw new AssertionError(); // Can never happen
		}
	}
	
	/**
	 * If there has been a socket error, this call could fail. If this happens,
	 * the caller should create another object and try again. (TODO correct?)
	 */
	void accept(long walIndex, AvroWALEntry entry, 
			RespWaiter<AvroAcceptResponse> waiter) {
		AvroAccept msg = new AvroAccept();
		msg.walEntry = entry;
		msg.reqSerial = getNextSerial();
		msg.walIndex = walIndex;
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		BinaryEncoder e = EncoderFactory.get().binaryEncoder(bao, null);
		try {
			bao.write(MSG_ACCEPT);
			acceptWriter.write(msg, e);
			e.flush();
			byte[] outgoingBytes = bao.toByteArray();
			prepWaiters.put(msg.reqSerial, waiter);
			schan.write(ByteBuffer.wrap(outgoingBytes));
		} catch (IOException ex) {
			waiter.nack(host);
		}
		
	}
	
	static class RespWaiter<T> {
		public Map<Host,T> responses;
		int validResponses = 0;
		int nackResponses = 0;
		int numExpected;
		Log logger = LogFactory.getLog(RespWaiter.class);
		
		public RespWaiter(int numExpected) {
			responses = new HashMap<Host,T>();
			this.numExpected = numExpected;
		}
		
		synchronized public void response(Host host, T response) {
			responses.put(host, response);
			validResponses++;
			notify();
		}
		
		synchronized public void nack(Host host) {
			responses.put(host, null);
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
