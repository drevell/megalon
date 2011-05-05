package org.megalon.multistageserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.util.ByteBufferInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The Avro ByteBufferInputStream doesn't implement available(), which we need.
 * So we subclass it here and add tracking of the number of bytes available.
 */
public class BBInputStream extends ByteBufferInputStream {
	int bytesRemaining;
	Log logger = LogFactory.getLog(BBInputStream.class);
	static final boolean traceEnabled = false;
	
	public BBInputStream(List<ByteBuffer> buffers) {
		super(buffers);
		bytesRemaining = 0;
		for(ByteBuffer bb: buffers) {
			bytesRemaining += bb.remaining(); 
		}
		logRemaining("constructor");
	}
	
	protected void logRemaining(String tag) {
		if(traceEnabled) {
			logger.debug(tag + ": BBInputStream has " + bytesRemaining + 
					" bytes remaining");
		}
	}

	public int available() throws IOException {
		logRemaining("available()");
		return bytesRemaining;
	}

	public int read() throws IOException {
		return super.read();
	}
	
	public int read(byte[] b, int off, int len) throws IOException {
		int bytesRead = super.read(b, off, len);
		bytesRemaining -= bytesRead;
		logRemaining("read(byte[],int,int)");
		return bytesRead;
	}

	public ByteBuffer readBuffer(int length) throws IOException {
		throw new IOException("readBuffer unsupported");
	}

	public int read(byte[] b) throws IOException {
		return this.read(b, 0, b.length);
	}
	
	public synchronized void reset() throws IOException {
		throw new IOException("reset() unsupported");
	}

	public long skip(long arg0) throws IOException {
		throw new IOException("skip() unsupported");
	}
}
