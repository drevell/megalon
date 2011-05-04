package org.megalon.multistageserver;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;
import org.megalon.multistageserver.SocketAccepter.PayloadFactory;

public class SocketPayload extends Payload {
	public SocketChannel sockChan;
	public int bytesExpected = 0;
	public boolean continueReading = false;

	public LinkedList<ByteBuffer> readBufs = new LinkedList<ByteBuffer>();
	public BBInputStream is = null;
	
	public ByteBuffer[] pendingOutput = null;
	public int curOutBuf;
	public int outBytesRemaining;
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
		while(readBufs.size() > 0 && readBufs.get(0).remaining() == 0) {
			readBufs.remove(0);
		}
		for(ByteBuffer bb: readBufs) {
			bb.flip();
		}
		pendingOutput = null;
		curOutBuf = -1;
		outBytesRemaining = -1;
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
