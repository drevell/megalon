package org.megalon.multistageserver;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.multistageserver.SocketAccepter.PayloadFactory;

public class SocketPayload extends Payload {
	Log logger = LogFactory.getLog(SocketPayload.class);
	
	public SocketChannel sockChan;
	public int bytesExpected = 0;
	public boolean continueReading = false;

	public LinkedList<ByteBuffer> readBufs = new LinkedList<ByteBuffer>();
	public BBInputStream is = null;
	
//	public List<ByteBuffer> pendingOutput = null;
//	private ByteBufferOutputStream os = new ByteBufferOutputStream();
	private Deque<ByteBuffer> outputQueue = new LinkedBlockingDeque<ByteBuffer>();
	private Lock outputAppendLock = new ReentrantLock();
	
	public boolean timedOut;
	
	public SocketPayload(SocketChannel sockChan) {
		this(sockChan, null);
	}
	
	public SocketPayload(SocketChannel sockChan, Payload outerPayload) {
		super(outerPayload);
		this.sockChan = sockChan;
	}
	
	static public class Factory implements PayloadFactory<SocketPayload> {
		public SocketPayload makePayload(SocketChannel sockChan) {
			return new SocketPayload(sockChan);
		}
	}
	
	/**
	 * When a selector receives data from a socket, or the selection timeout for
	 * this payload has been reached, this function will be called. Subclasses
	 * should override this to return true in the cases where the selector 
	 * should keep select'ing on this payload's socket, and return false in the
	 * cases where the selector should pass the payload on to the next stage
	 * (and not select on that socket anymore). 
	 * 
	 * Subclasses should make sure this function returns *quickly*, since it's
	 * executed by the selector thread between calls to select().
	 */
	public boolean keepSelecting(boolean timedOut) {
		return false;
	}
	
	public boolean didTimeOut() {
		return timedOut;
	}
	
	/**
	 * Queue up some output buffers to be eventually written to the socket.
	 */
	public void enqueueOutput(List<ByteBuffer> outBufs) {
		try {
			outputAppendLock.lock();
			outputQueue.addAll(outBufs);
		} finally {
			outputAppendLock.unlock();
		}
	}
	
	/**
	 * Returns the ByteBuffer at the head of the output queue without dequeueing
	 * it, or null if the queue is empty.
	 */
	public ByteBuffer peekPendingOutput() {
		return outputQueue.peek();
	}
	
	/**
	 * Dequeues and returns the next ByteBuffer that's pending output, or null
	 * if there are no ByteBuffers pending output.
	 */
	public ByteBuffer pollPendingOutput() {
		return outputQueue.poll();
	}
	
	/**
	 * Pushes a ByteBuffer onto the head (NOT TAIL) of the pending output queue.
	 */
	public void pushPendingOutput(ByteBuffer bb) {
		outputQueue.push(bb);
	}
//	/**
//	 * To send data back to the client, Megalon stages can use the OutputStream
//	 * returned by this function.
//	 */
//	public ByteBufferOutputStream getOutputStream() {
//		return os;
//	}
}
