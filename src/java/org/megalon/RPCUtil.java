package org.megalon;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RPCUtil {
	static Log logger = LogFactory.getLog(RPCUtil.class);
	
	// We need at a length field, a serial field, and a msgType
	public static final int RPC_HEADER_SIZE = Integer.SIZE/8 + Long.SIZE/8 + 1;
	
	/**
	 * Look at the incoming ByteBuffers, and check whether we have a complete
	 * message. That is, assuming the first four available bytes are a length
	 * prefix, check if at least that many bytes are available.
	 * 
	 * @throws IOException if the input buffer is misformatted
	 */
	static public boolean hasCompleteMessage(List<ByteBuffer> readBufs) 
	throws IOException {
		if(readBufs == null || readBufs.size() == 0) {
			return false;
		}
		int totalBytesAvailable = 0;
		for(ByteBuffer bb: readBufs) {
			totalBytesAvailable += bb.remaining();
		}

		logger.debug("totalBytesAvailable: " + totalBytesAvailable);
		if(totalBytesAvailable < RPC_HEADER_SIZE) {
			// There aren't enough bytes for a header, so definitely no message
			return false;
		}
		byte[] lengthBytes = new byte[4];
		int bytesGotten = 0;
		for(ByteBuffer bb: readBufs) {
			logger.debug("Started a new buffer");
			int startPos = bb.position();
			int bytesRemainingToGet = 4 - bytesGotten;
			int bytesAvailableThisBuffer = bb.remaining();
			int bytesToGetThisBuffer = Math.min(bytesRemainingToGet, 
					bytesAvailableThisBuffer);
			for(int i=0; i<bytesToGetThisBuffer && bytesGotten < 4; i++) {
				lengthBytes[bytesGotten] = bb.get(startPos+i);
				bytesGotten++;
			}
			if(bytesGotten == 4) {
				int msgLengthField = Util.bytesToInt(lengthBytes);
				if(msgLengthField < RPC_HEADER_SIZE-4) {
					throw new IOException("Message was too short to contain " +
							"required fields");
				}
				logger.debug("msgLengthField: " + msgLengthField);
				return totalBytesAvailable-4 >= msgLengthField;
			}
		}
		return false;
	}
	
	/**
	 * For debugging only, get the contents of some bytebuffers as a String.
	 */
	static public String strBufs(List<ByteBuffer> bufs) {
		StringBuilder sb = new StringBuilder();
		for(ByteBuffer bb: bufs) {
			ByteBuffer logBb = bb.duplicate();
			byte[] bytes = new byte[logBb.remaining()];
			logBb.get(bytes);
			sb.append(Arrays.toString(bytes));
		}
		return sb.toString();
	}
	
	/**
	 * Read an int from an InputStream.
	 */
	static int readInt(InputStream is) throws IOException {
		byte[] buf = new byte[Integer.SIZE/8];
		is.read(buf);
		return Util.bytesToInt(buf);
	}

	/**
	 * Read an long from an InputStream.
	 */
	static long readLong(InputStream is) throws IOException {
		byte[] buf = new byte[Long.SIZE/8];
		is.read(buf);
		return Util.bytesToLong(buf);
	}
}
