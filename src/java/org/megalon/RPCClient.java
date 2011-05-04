package org.megalon;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config.Host;
import org.megalon.multistageserver.BBInputStream;

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
 * TODO update documentation
 */
public class RPCClient {
	public static final int BUFFER_SIZE = 16384;
	public static final long reconnectAttemptIntervalMs = 5000;
	Log logger = LogFactory.getLog(RPCClient.class);
	
	SocketChannel schan;
	long reqSerial = 0;
	Host host;
	String replica;
	Thread reader;
	long lastReconnectAttemptTime = 0;
	ByteBufferOutputStream os = new ByteBufferOutputStream();
	
	Map<Long, MPaxPayload> payloads = new ConcurrentHashMap<Long, MPaxPayload>();
	PriorityQueue<TimeoutPair> timeoutPq = new PriorityQueue<TimeoutPair>(10, 
			new TimeoutPair.TPComparator());
	
	LinkedList<ByteBuffer> outBufs = new LinkedList<ByteBuffer>();
	LinkedList<ByteBuffer> readBuffers = new LinkedList<ByteBuffer>();
	Selector selector;
	
//	class PayloadTimeoutComparator implements Comparator<MPaxPayload> {
//		public int compare(MPaxPayload p1, MPaxPayload p2) {
//			return Long.valueOf(p1.finishTimeMs).compareTo(p2.finishTimeMs);
//		}		
//	}
	
	public RPCClient(Host host, String replica) {
		this.host = host;
		this.replica = replica;
		reader = new Thread() {
			public void run() {
				while(true) {
					selectLoop();
				}
			}
		};
		reader.setDaemon(true);
		reader.start();
	}
	
	/**
	 * Add a list of ByteBuffers to the pending output queue.
	 * TODO limit the size of the output buffer
	 */
	public boolean write(List<ByteBuffer> outBufs, MPaxPayload payload) {
		if(!ready()) {
			return false;
		}
		
		long reqSerial = getNextSerial();
			
		// Prepend the request serial ID and message length
		outBufs.add(0, ByteBuffer.wrap(Util.longToBytes(reqSerial)));

		int length = 0;
		for(ByteBuffer bb: outBufs) {
			length += bb.remaining();
			logger.debug("length " + length);
		}
		outBufs.add(0, ByteBuffer.wrap(Util.intToBytes(length)));
		
		logger.debug("Outgoing bufs: " + RPCUtil.strBufs(outBufs));
		
		// Set up the caller to receive the response, when it arrives
		synchronized(this) {
			payloads.put(reqSerial, payload);
			timeoutPq.add(new TimeoutPair(payload.finishTimeMs, reqSerial));
			this.outBufs.addAll(outBufs); // Add output to queue
		}
		
		// Wake up the selector to send the write
		selector.wakeup();
		return true;
	}
	
	/**
	 * Open (or re-open) a socket connection to the remote replication server.
	 * This has the side effect of clearing all the state, so all outstanding
	 * requests will be immediately nacked.
	 */
	protected boolean reconnect() {
		// Throttle reconnection attempts
//		long nowTimeMs = System.currentTimeMillis();
//		if(lastReconnectAttemptTime + reconnectAttemptIntervalMs <= nowTimeMs) {
//			return false;
//		}
//		lastReconnectAttemptTime = nowTimeMs;
		
		synchronized(this) {
			try {
				failAllOutstanding();
				selector = Selector.open();
				schan = SocketChannel.open(new InetSocketAddress(
						host.nameOrAddr, host.port));
				schan.configureBlocking(false);
				outBufs.clear();
				readBuffers.clear();
				logger.debug("RPCClient connected to " + host);
				return true;
			} catch (Exception e) {
				logger.info("RPCClient connection to " + host + 
						" had exception", e);
				return false;
			}
		}
	}
	
	/**
	 * This will call nack() on all pending requests. We have to do this if we
	 * lose our connection to the remote server.
	 */
	synchronized void failAllOutstanding() {
		while(true) {
			TimeoutPair timeoutPair = timeoutPq.poll();
			if(timeoutPair == null) {
				break;
			}
			MPaxPayload payload = payloads.get(timeoutPair.reqSerial);
			payload.replResponses.nack(this.replica);
		}
		payloads.clear();
	}
	
	boolean ready() {
		return schan != null && schan.isConnected() && selector != null;
	}

	/**
	 * Loop forever, reading and writing to the socket when possible, and
	 * reconnecting if there's an IOException.
	 */
	protected void selectLoop() {
		while(true) {
			try {
				if(!ready()) {
					logger.debug("To reconnect");
					if(!reconnect()) {
						logger.debug("Reconnect failed, sleeping");
						Util.sleep(5000);
						continue;
					}
				}
				long timeout;
				// We're interested in sock writability iff there's queued output 
				synchronized(this) {
					int interestOps;
					if(outBufs.size() > 0) {
						interestOps = SelectionKey.OP_READ | SelectionKey.OP_WRITE; 
					} else {
						interestOps = SelectionKey.OP_READ;
					}
					schan.register(selector, interestOps);
					
					TimeoutPair soonestTimeout = timeoutPq.peek(); 
					if(soonestTimeout != null) {
						long nowTimeMs = System.currentTimeMillis();
						timeout = soonestTimeout.finishTimeMs - nowTimeMs;
					} else {
						timeout = Long.MAX_VALUE;
					}
				}
				
				if(timeout >= 0) {
					selector.select(timeout);
				}
				
				// There's only one socket (so only one key)
				Iterator<SelectionKey> keyIt = selector.selectedKeys().iterator();
				while(keyIt.hasNext()) {
					SelectionKey key = keyIt.next();
					keyIt.remove();
					if(key.isReadable()) {
						handleReadable();
					}
					if(key.isWritable()) {
						handleWritable();
					}
				}
				
				timeOutReads();
			} catch (IOException e) {
				logger.warn("selectLoop IOException", e);
				try {
					schan.close();
					schan = null;
				} catch (IOException ex) {}
			}
		} 
	}
	
	/**
	 * From our queued list of ByteBuffers pending output, write data until the
	 * socket stops accepting any more.
	 * TODO document why things are synchronized in this class
	 */
	synchronized void handleWritable() throws IOException {
		int bytesWritten;
		do {
			if(outBufs.size() == 0) {
				break;
			}
			ByteBuffer bb = outBufs.get(0); 
			bytesWritten = schan.write(bb);
			if(bb.remaining() == 0) {
				outBufs.removeFirst(); // TODO GC pressure
			}
		} while(bytesWritten > 0);
	}
	
	/**
	 * Check the head of the priority queue for operations that timed out
	 * recently, and call nack() on their payloads.
	 */
	void timeOutReads() {
		long nowTimeMs = System.currentTimeMillis();
		while(true) { 
			synchronized(this) {
				TimeoutPair soonestTimeout = timeoutPq.peek();
				if(soonestTimeout == null) {
					return;
				}
				
				if(soonestTimeout.finishTimeMs > nowTimeMs) {
					break;
				}
				TimeoutPair timeoutPair = timeoutPq.remove();
				MPaxPayload payload = payloads.get(timeoutPair.reqSerial);
				payload.replResponses.nack(replica);
			}
		}
	}
		
	/**
	 * This is called when the socket is readable (there's only one socket).
	 * @throws IOException
	 */
	void handleReadable() throws IOException {
		logger.debug("In handleReadable");
//		ByteBuffer buf = readBuffers.getLast();
//		if(buf.remaining() == 0) {
//			buf = ByteBuffer.allocate(BUFFER_SIZE);
//			readBuffers.add(buf);
//		}
		
		// Read all available bytes from the socket into a list of ByteBuffers
		while(true) {
			logger.debug("hr1");
			ByteBuffer buf = ByteBuffer.allocate(BUFFER_SIZE);
			logger.debug("hr2");
			int bytesRead = schan.read(buf);
			logger.debug("hr bytesRead:" + bytesRead);
			if(bytesRead == -1) {
				throw new IOException("End of stream");
			} else if(bytesRead == 0) {
				break;
			} else {
				buf.flip();
				readBuffers.add(buf);
				if(buf.limit() != buf.capacity()) {
					// The last buffer we read was not filled, so there must not
					// be any more bytes in the socket
					
					break;
				}
			}
			
			logger.debug("hr2.5");
		}
		// If any complete responses are in the incoming buffers, send them to
		// the objects that are waiting for them.
		logger.debug("readBuffers.size(): " + readBuffers.size());
		dispatchResponses(readBuffers);
	}
	
	/**
	 * Given a list of ByteBuffers containing zero or more incoming length-
	 * prefixed messages, read the messages and send the messages to their 
	 * respective waiting payload objects.
	 */
	void dispatchResponses(List<ByteBuffer> readBufs) throws IOException {
		InputStream is = null;
		byte[] reqSerialBuf = null;
		byte[] msgLenBuf = null;
		logger.debug("dispatchResponses" + RPCUtil.strBufs(readBufs));
		while(RPCUtil.hasCompleteMessage(readBufs)) {
			if(is == null) {
				is = new BBInputStream(readBufs);
			}
			if(reqSerialBuf == null) {
				reqSerialBuf = new byte[Long.SIZE/8];
			}
			if(msgLenBuf == null) {
				msgLenBuf = new byte[Integer.SIZE/8];
			}
			
			int nBytesRead = is.read(msgLenBuf);
			assert nBytesRead == Integer.SIZE/8;
			int msgLen = Util.bytesToInt(msgLenBuf);
			
			assert is.available() > msgLen;
			is.read(reqSerialBuf);
			long reqSerial = Util.bytesToLong(reqSerialBuf); 
			byte[] response = new byte[msgLen]; // TODO GC pressure
			is.read(response);
			synchronized(this) {
				MPaxPayload payload = payloads.get(reqSerial);
				if(payload != null) {
					payloads.remove(reqSerial);
					timeoutPq.remove(payload);
				}
				payload.replResponses.ack(replica, response);
			}
		}
	}
	
//	/**
//	 * Reset the input buffer to its pristine state: a list containing one empty
//	 * ByteBuffer ready for reading.
//	 */
//	void resetReadBuffers() {
//		ByteBuffer onlyBuffer;
//		int numReadBuffers = readBuffers.size();
//		if(numReadBuffers > 1) {
//			onlyBuffer = readBuffers.get(0);
//			onlyBuffer.clear();
//			readBuffers.clear();
//			readBuffers.add(onlyBuffer);
//		} else if (numReadBuffers == 0) {
//			onlyBuffer = ByteBuffer.allocate(BUFFER_SIZE);
//			readBuffers.add(onlyBuffer);
//		} else {
//			readBuffers.get(0).clear();
//		}
//	}
	
	protected synchronized long getNextSerial() {
		reqSerial++;
		return reqSerial;
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
	
	/**
	 * To keep a priority queue that gives us the soonest timeout, we need a
	 * class that sorts by finishing time and points to a request serial number
	 * of the request that's timing out. This is that class. This is the type
	 * of objects that go into the priority queue.
	 */
	static class TimeoutPair {
		long finishTimeMs, reqSerial;
		
		public TimeoutPair(long finishTimeMs, long reqSerial) {
			this.finishTimeMs = finishTimeMs;
			this.reqSerial = reqSerial;
		}
		
		/**
		 * This comparator allows the PriorityQueue to sort in order of timeout:
		 * the next object in the PQ will be the soonest request to timeout.
		 */
		static class TPComparator implements Comparator<TimeoutPair> {
			public int compare(TimeoutPair l, TimeoutPair r) {
				return Long.valueOf(l.finishTimeMs).compareTo(r.finishTimeMs);
			}
		}
	}
}
