package org.megalon.multistageserver;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import org.megalon.multistageserver.MultiStageServer.Finisher;

/**
 * This is the parent class for objects that are passed between the stages of
 * the server. They contain request state.
 * 
 * There are two ways to run code after request handling completes. One way is
 * to set a "finisher," which is an object implementing finish(payload), which
 * will be run after the last server stage completes. The other way is to call
 * waitFinished(), which will block the calling thread until the server
 * completes and calls notifyAll().
 */
public class Payload implements Finisher<Payload> {
	private volatile boolean isFinished = false;
	public Throwable throwable;
	@SuppressWarnings("rawtypes")
	public Finisher finisher;

	// If a server recursively uses another server, this will hold a linked
	// list of payloads that belong to the outer servers.
	protected Payload outerPayload;

	// We assign to this volatile variable to prevent the compiler from
	// reordering memory operations between stages (memory fence).
	protected volatile boolean barrier;

	// public Payload(SocketChannel sockChan) {
	// this(sockChan, null);
	// }
	//
	// public Payload(SocketChannel sockChan, Payload outerPayload) {
	// this.sockChan = sockChan;
	// this.outerPayload = outerPayload;
	// }

	public Payload() {
		this(null);
	}

	public Payload(Payload outerPayload) {
		this.outerPayload = outerPayload;
	}

	/**
	 * Block until all server stages have finished processing.
	 */
	synchronized public void waitFinished() {
		while (!isFinished) {
			try {
				wait();
			} catch (InterruptedException e) {
			}
		}
	}

	synchronized public void waitFinished(long timeout, TimeUnit unit) {
		long timeoutMs = TimeUnit.MILLISECONDS.convert(timeout, unit);
		while (!isFinished) {
			try {
				wait(timeoutMs);
			} catch (InterruptedException e) {
			}
		}
	}

	/**
	 * Called by a StageRunner when the final stage is done to indicate
	 * completion.
	 * 
	 * @param e any exception that may have occurred in the last stage
	 */
	synchronized void setFinished(Throwable e) {
		this.isFinished = true;
		this.throwable = e;
		notifyAll();
	}

	/**
	 * Called by StageRunner to indicate error-free completion of server
	 * processing.
	 */
	void setFinished() {
		setFinished(null);
	}

	/**
	 * Implements the Finisher interface to that a payload can act as its own
	 * finisher.
	 */
	public void finish(Payload payload) {
		if (payload != this) {
			throw new AssertionError("A payload cannot be a finisher for a "
					+ "different payload");
		}
		setFinished();
	}

	/**
	 * Get any wrapped payload that may be contained in this payload. This is
	 * used when one server calls another server and pushes its payload into a
	 * payload stack.
	 */
	public Payload getOuterPayload() {
		return outerPayload;
	}
	
	public boolean finished() {
		return isFinished;
	}
}