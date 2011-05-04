package org.megalon;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config.Host;

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
	public static final int INT_NBYTES = Integer.SIZE / 8;
	public static final int LONG_NBYTES = Long.SIZE / 8;
	
	Log logger = LogFactory.getLog(RPCClient.class);
	
	SocketChannel schan;
	long reqSerial = 0;
	Host host;
	String replica;
	Thread reader;
	long lastReconnectAttemptTime = 0;
	ByteBufferOutputStream os = new ByteBufferOutputStream();
	
	Map<Long, Waiter> reqBySerial = new HashMap<Long, Waiter>();
	TreeSet<Waiter> reqByTimeout = new TreeSet<Waiter>(new Waiter.WaiterCmp());
	
	LinkedList<ByteBuffer> outBufs = new LinkedList<ByteBuffer>();
	LinkedList<ByteBuffer> readBuffers = new LinkedList<ByteBuffer>();
	Selector selector;
	
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
			Waiter waiter = new Waiter(payload.finishTimeMs, reqSerial, payload);
			reqBySerial.put(reqSerial, waiter);
			reqByTimeout.add(waiter);
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
		for(Waiter waiter: reqByTimeout) {
			reqBySerial.remove(waiter.reqSerial);
			waiter.payload.replResponses.nack(this.replica);
		}
		reqByTimeout.clear();
		assert reqBySerial.size() == 0 : "Inconsistent lookup structures";
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
					
					 
					if(reqByTimeout.isEmpty()) {
						timeout = Long.MAX_VALUE;
					} else {
						long nowTimeMs = System.currentTimeMillis();
						timeout = reqByTimeout.first().finishTimeMs - nowTimeMs;						
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
		synchronized(this) {
			Iterator<Waiter> it = reqByTimeout.iterator();
			while(it.hasNext()) {
				Waiter waiter = it.next();
				
				if(waiter.finishTimeMs > nowTimeMs) {
					break;
				}
				waiter.payload.replResponses.nack(replica);
				
				// Remove the timed out waiter from our tracking structures
				it.remove();
				reqBySerial.remove(waiter.reqSerial);
			}
		}
	}
		
	/**
	 * This is called when the socket is readable (there's only one socket).
	 * @throws IOException
	 */
	void handleReadable() throws IOException {
		
		// Read all available bytes from the socket into a list of ByteBuffers
		while(true) {
			ByteBuffer buf = ByteBuffer.allocate(BUFFER_SIZE);
			int bytesRead = schan.read(buf);
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
		logger.debug("dispatchResponses" + RPCUtil.strBufs(readBufs));
		while(RPCUtil.hasCompleteMessage(readBufs)) {
			int msgLen = Util.bytesToInt(RPCUtil.extractBytes(INT_NBYTES, 
					readBufs));
			long reqSerial = Util.bytesToLong(RPCUtil.extractBytes(LONG_NBYTES,
					readBufs));
			
			// Copy the message body to the waiting payload 
			int bytesToCopy = msgLen - LONG_NBYTES;
			List<ByteBuffer> response = RPCUtil.extractBufs(bytesToCopy, readBufs);
			Waiter waiter;
			synchronized(this) {
				waiter = reqBySerial.remove(reqSerial);
				if(waiter == null) {
					logger.debug("Response for timed-out request (fine)");
				} else {
					boolean didRemove = reqByTimeout.remove(waiter);
					assert didRemove;
				}
			}
			waiter.payload.replResponses.ack(replica, response);
		}
	}

	protected synchronized long getNextSerial() {
		reqSerial++;
		return reqSerial;
	}
	
	/**
	 * To keep a priority queue that gives us the soonest timeout, we need a
	 * class that sorts by finishing time and points to a request serial number
	 * of the request that's timing out. This is that class. This is the type
	 * of objects that go into the priority queue.
	 */
	static class Waiter {
		long finishTimeMs, reqSerial;
		MPaxPayload payload;
		
		public Waiter(long finishTimeMs, long reqSerial, MPaxPayload payload) {
			this.finishTimeMs = finishTimeMs;
			this.reqSerial = reqSerial;
			this.payload = payload;
		}
		
		/**
		 * For Waiters to be stored in TreeSets correctly, equals() must be
		 * consistent with the WaiterCmp comparator (below). This is easy, we
		 * just say two Waiters are equal iff their fields are equal.
		 */
		public boolean equals(Waiter other) {
			return (reqSerial == other.reqSerial) && 
				(finishTimeMs == other.finishTimeMs);
		}
		
		/**
		 * This comparator allows the TreeSet to sort in order of timeout:
		 * the first object in comparator order will be the soonest request to 
		 * time out.
		 */
		static class WaiterCmp implements Comparator<Waiter> {
			public int compare(Waiter l, Waiter r) {
				// Order first by soonest finishing time
				int finishCmpVal = 
					Long.valueOf(l.finishTimeMs).compareTo(r.finishTimeMs);
				if(finishCmpVal != 0) {
					return finishCmpVal;
				}
				// If finishing times are equal, then break ties by reqSerial
				return Long.valueOf(l.reqSerial).compareTo(r.reqSerial);
			}
		}
	}
}
