package org.megalon.multistageserver;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.util.ByteBufferOutputStream;
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
	
	public List<ByteBuffer> pendingOutput = null;
	public ByteBufferOutputStream os = new ByteBufferOutputStream();
	
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
	
	public void resetForRead() {
		if(!continueReading) {
			logger.debug("resetForRead: clearing readBufs");
			readBufs.clear();
		} else {
			logger.debug("resetForRead: not clearing readBufs");
		}
		pendingOutput = null;
		os.reset();
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
}
