package org.megalon.multistageserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.RPCUtil;
import org.megalon.multistageserver.MultiStageServer.NextAction;
import org.megalon.multistageserver.MultiStageServer.NextAction.Action;
import org.megalon.multistageserver.MultiStageServer.Stage;

public class SelectorStage<T extends SocketPayload> implements Stage<T> {
	// The maximum incoming request size is X megabytes TODO configurable
	public static final int MAX_READ_BUF = 50 * (int)Math.pow(2, 20);
	public static final int BUF_SIZE = 32768;
	
	// The maximum number of payloads waiting to be registered with the selector
	public static final int MAX_QUEUED = 100;
	
	Log logger = LogFactory.getLog(SelectorStage.class);
	Stage<T> nextStage;
	Selector selector;
	int numConcurrent;
	int backlogSize;
	String name;
	Thread selectThread;
	MultiStageServer<T> server;
	Queue<T> payloadsPendingSelect = new LinkedBlockingQueue<T>(MAX_QUEUED);
	
	public SelectorStage(Stage<T> nextStage, String name, int numConcurrent,
			int backlogSize) throws IOException {
		this.nextStage = nextStage;
		selector = Selector.open(); // Can throw IOExc on filehandle exhaustion
		this.numConcurrent = numConcurrent;
		this.backlogSize = backlogSize;
		this.name = name;
		selectThread = new SelectThread();
		selectThread.start();
	}
	
	public NextAction<T> runStage(T payload) throws Exception {
		if(payload.sockChan.isBlocking()) {
			throw new IOException("SelectorStage needs non-blocking sockets");
		}
		logger.debug("Adding payload to pending select queue");
		payloadsPendingSelect.add(payload);
		logger.debug("Sending wakeup");
		selector.wakeup();
		logger.debug("Sent wakeup");
		return new NextAction<T>(Action.IGNORE, null);
	}

	public int getNumConcurrent() {
		return numConcurrent;
	}

	public String getName() {
		return name;
	}

	public int getBacklogSize() {
		return backlogSize;
	}
	
	public void setServer(MultiStageServer<T> server) {
		this.server = server;
	}

	class SelectThread extends Thread {
		public SelectThread() {
			this.setDaemon(true);
		}
		
		public void run() {
			long lastIOErrMsgTime = 0;
			while(true) {
				try {
					logger.debug("Calling select()");
					selector.select();
					logger.debug("select() returned");
				} catch (IOException e) {
					// Only log every 10 seconds, don't spam the log
					if(System.currentTimeMillis() - lastIOErrMsgTime > 10000) {
						logger.error("Selector IOException", e);
						lastIOErrMsgTime = System.currentTimeMillis();
					}
				}
				
				Iterator<SelectionKey> keyIt = selector.selectedKeys().iterator(); 
				while(keyIt.hasNext()) {
					SelectionKey key = keyIt.next();
					handleReadyKey(selector, key);
					keyIt.remove();
				}
				
				// There may be payloads newly arrived at this stage that
				// should be added to the selector.
				for(T payload: payloadsPendingSelect) {
					int interestOps = SelectionKey.OP_READ;
					logger.debug("Adding socket to the selector: " + 
							payload.sockChan.socket().getInetAddress() + ":" +
							payload.sockChan.socket().getPort());
					if(payload.peekPendingOutput() != null) {
						logger.debug("Selecting for writability");
						interestOps |= SelectionKey.OP_WRITE;
					}

					try {
						payload.sockChan.register(selector, interestOps, payload);
					} catch (IOException ex) {
						logger.debug("IOException on selector registration", ex);
						server.finishPayload(payload, ex);
					}
					logger.debug("Done adding socket to the selector");
					payloadsPendingSelect.remove(payload);
				}
			}
		}
	}

	/**
	 * When the selector reports that a channel is ready to read or write, 
	 * this will be called. It will write any pending data if it exists, 
	 * otherwise it will attempt to read.
	 */
	void handleReadyKey(Selector selector, SelectionKey key) {
		SocketChannel schan = (SocketChannel)key.channel();
		@SuppressWarnings("unchecked")
		T payload = (T)key.attachment();
		assert payload != null;
		
		Throwable maybeException = null;
		boolean isEndOfStream = false;
		try {
			if(key.isWritable()) {
				logger.debug("SelectorStage: socket is writable");
//				List<ByteBuffer> outBufs = payload.pendingOutput;
//				ListIterator<ByteBuffer> bufIt = outBufs.listIterator();
//				while(bufIt.hasNext()) {
				while(true) {
					ByteBuffer bb = payload.pollPendingOutput();
					if(bb == null) {
						break;
					}
//					ByteBuffer bb = bufIt.next();
					int numRemaining = bb.remaining();
					if(numRemaining == 0) {
						continue;
					}
					logger.debug("To SocketChannel.write() with " + 
							RPCUtil.strBuf(bb));
					int numWritten = schan.write(bb);
					if(numWritten != numRemaining) {
						// We didn't write all the data from this ByteBuffer,
						// so write more data later
						payload.pushPendingOutput(bb);
						break;
					}
				}
			} 
			
			if(key.isReadable()) {
				// Read all available bytes from the socket as a list of ByteBuffers.
				// TODO reuse existing ByteBuffers

				int totalBytesRead = 0;
				ByteBuffer bb = null;
				do {
					bb = ByteBuffer.allocate(BUF_SIZE);
					int bytesThisRead = schan.read(bb);
					if(bytesThisRead == 0) {
						break;
					} else if(bytesThisRead == -1) {
						logger.debug("SelectorStage end of stream");
						isEndOfStream = true;
						break;
					}
					totalBytesRead += bytesThisRead;
					payload.readBufs.add(bb);
					bb.flip();
					logger.debug("Got " + bytesThisRead + " bytes this read: " +
							RPCUtil.strBuf(bb));
				} while(bb.hasRemaining());
				logger.debug("Read " + totalBytesRead + " total bytes");
				if(totalBytesRead == 0) {
					logger.debug("Checking if connected");
					if(!schan.isConnected()) {
						logger.debug("Socket disconnected, closing");
						isEndOfStream = true;
					}
				} else {
					payload.is = new BBInputStream(payload.readBufs);
					logger.debug("SelectorStage sending payload with " + 
							payload.is.available() + " bytes");
					server.enqueue(payload, nextStage, payload.finisher);
				}
			}

			// We're interested in writability on this socket in the future if
			// and only if there's pending output.
			int interestOps;
			if(payload.peekPendingOutput() == null) {
				logger.debug("No pending output for socket, selecting for " +
						"read only");
				interestOps = SelectionKey.OP_READ;
			} else {
				logger.debug("Pending output for socket, selecting for " +
						"writability");
				interestOps = SelectionKey.OP_READ | SelectionKey.OP_WRITE;
				schan.register(selector, interestOps, payload);
			}
			schan.register(selector, interestOps, payload);

		} catch (Exception e) {
			maybeException = e;
		}
		
		if(maybeException != null || isEndOfStream) {
			if(maybeException != null) {
				logger.debug("handleReadyKey exception", maybeException);
			}
			key.cancel();
			server.finishPayload(payload, maybeException);
		}
	}
}
