package org.megalon.multistageserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.util.ByteBufferInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.multistageserver.MultiStageServer.NextAction;
import org.megalon.multistageserver.MultiStageServer.NextAction.Action;
import org.megalon.multistageserver.MultiStageServer.Stage;

public class SelectorStage<T extends SocketPayload> implements Stage<T> {
	// The maximum incoming request size is X megabytes TODO configurable
	public static final int MAX_READ_BUF = 50 * (int)Math.pow(2, 20);
	public static final int BUF_SIZE = 32768;
	
	Log logger = LogFactory.getLog(SelectorStage.class);
	Stage<T> nextStage;
	Selector selector;
	int numConcurrent;
	int backlogSize;
	String name;
	Thread selectThread;
	MultiStageServer<T> server;
	Map<T,Integer> payloadsPendingSelect = new ConcurrentHashMap<T,Integer>();
	
	public SelectorStage(Stage<T> nextStage, String name, int numConcurrent,
			int backlogSize) throws IOException {
		this.nextStage = nextStage;
		selector = Selector.open(); // Can this actually throw IOExc? Why?
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
		int interestOps;
		List<ByteBuffer> outBufs = payload.os.getBufferList(); 
		if(outBufs.size() >= 1 && outBufs.get(0).limit() != 0) {
			logger.debug("Payload has data pending write, select for write");
			// There is data pending write, select only for writability now.
			interestOps = SelectionKey.OP_WRITE;
			payload.pendingOutput = outBufs.toArray(new ByteBuffer[0]);
			payload.curOutBuf = 0;
			payload.outBytesRemaining = 0;
			for(ByteBuffer bb: payload.pendingOutput) {
				payload.outBytesRemaining += bb.remaining();
			}
			logger.debug("Num bytes to write: " + payload.outBytesRemaining);
		} else {
			// No data pending write, so wait for an incoming request.
			logger.debug("Payload has no data pending write, select for read");
			interestOps = SelectionKey.OP_READ;
			payload.resetForRead();
		}
		payloadsPendingSelect.put(payload, interestOps);
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
				for(Map.Entry<T,Integer> e: payloadsPendingSelect.entrySet()) {
					T payload = e.getKey();
					int interestOps = e.getValue();
					logger.debug("Adding a socket to the selector");
					try {
						payload.sockChan.register(selector, interestOps, payload);
					} catch (ClosedChannelException ex) {
						server.finishPayload(payload, ex);
					}
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
		
		Throwable maybeException = null;
		boolean isEndOfStream = false;
		try {
			if(key.isWritable()) {
				logger.debug("SelectorStage: socket is writable");
				ByteBuffer[] outBufs = payload.pendingOutput;
				// These contortions write a ByteBuffer array while handling
				// partial writes and updating the next starting position.
				int bytesRemainingThisBuf =	
					outBufs[payload.curOutBuf].remaining();
				logger.debug("bytesRemainingThisBuf: " + bytesRemainingThisBuf);
				long bytesWritten;
				try {
					logger.debug("Writing socket now, curOutBuf " + 
							payload.curOutBuf);
					bytesWritten = schan.write(outBufs, payload.curOutBuf, 
							outBufs.length);
					logger.debug("Socket write OK");
				} catch (IOException e) {
					logger.warn("SelectorStage exception during write", e);
					throw e;
				}
				logger.debug("Wrote " + bytesWritten + "/" + 
						payload.outBytesRemaining + " bytes");
				if(bytesWritten == payload.outBytesRemaining) {
					// All output has been written. Now select for readability
					logger.debug("Finished writing output buffers");
					payload.resetForRead();
					schan.register(selector, SelectionKey.OP_READ, payload);	
				} else {
					logger.debug("Write only partially succeeded");
					payload.outBytesRemaining -= bytesWritten;
					if(bytesWritten >= bytesRemainingThisBuf) {
						payload.curOutBuf++; // We finished writing this buffer
						logger.debug("Finished a buffer");
						bytesWritten -= bytesRemainingThisBuf;
						while(true) {
							int bufCapacity = outBufs[payload.curOutBuf].capacity();
							if(bufCapacity >= bytesWritten) {
								// 	We finished writing the next buffer also
								bytesWritten -= bufCapacity;
								payload.curOutBuf++; 
								logger.debug("Finished an additional buffer");
							} else {
								break;
							}
						}
					}	
				}
			} else if(key.isReadable()) {
				// Read all available bytes from the socket as a list of ByteBuffers.
				// TODO reuse existing ByteBuffers
				logger.debug("SelectorStage: socket is readable");
				int unusedBytes;
				int totalBytesRead = 0;
				ByteBuffer bb = null;
				LinkedList<ByteBuffer> readBufs = payload.readBufs;
				if(readBufs.size() > 0 && readBufs.getLast().remaining() > 0) {
					bb = payload.readBufs.get(0);
				}
				do {
					bb = ByteBuffer.allocate(BUF_SIZE);
					int bytesThisRead = schan.read(bb);
					if(bytesThisRead != -1) {
						totalBytesRead += bytesThisRead;
					}
					logger.debug("Got " + bytesThisRead + " bytes this read");
					if(bytesThisRead == 0) {
						break;
					} else if(bytesThisRead == -1) {
						logger.debug("SelectorStage end of stream");
						isEndOfStream = true;
						break;
					}
					unusedBytes = bb.remaining(); 
					payload.readBufs.add(bb);
					bb.flip();
				} while(unusedBytes == 0);
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
		} catch (Exception e) {
			maybeException = e;
		}
		
		if(maybeException != null || isEndOfStream) {
			logger.debug("handleReadyKey exception", maybeException);
			key.cancel();
			server.finishPayload(payload, maybeException);
		}
	}
}
