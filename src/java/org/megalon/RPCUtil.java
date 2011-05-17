package org.megalon;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
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

		//logger.debug("totalBytesAvailable: " + totalBytesAvailable);
		if(totalBytesAvailable < RPC_HEADER_SIZE) {
			// There aren't enough bytes for a header, so definitely no message
			return false;
		}
		byte[] lengthBytes = new byte[4];
		int bytesGotten = 0;
		for(ByteBuffer bb: readBufs) {
			//logger.debug("Started a new buffer");
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
				//logger.debug("msgLengthField: " + msgLengthField);
				return totalBytesAvailable-4 >= msgLengthField;
			}
		}
		return false;
	}
	
	/**
	 * For debugging only, get the contents of some bytebuffers as a String.
	 */
	static public String strBufs(List<ByteBuffer> bufs) {
		if(bufs == null) {
			return "(null)";
		}
		StringBuilder sb = new StringBuilder();
		for(ByteBuffer bb: bufs) {
			sb.append(strBuf(bb));
//			ByteBuffer logBb = bb.duplicate();
//			byte[] bytes = new byte[logBb.remaining()];
//			logBb.get(bytes);
//			sb.append(Arrays.toString(bytes));
		}
		return sb.toString();
	}
	
	static public String strBuf(ByteBuffer bb) {
		ByteBuffer dupedBb = bb.duplicate();
		byte[] bytes = new byte[dupedBb.remaining()];
		dupedBb.get(bytes);
		return Arrays.toString(bytes);
	}
	
	/**
	 * Read an int from an InputStream.
	 * @deprecated Array allocation causes GC pressure
	 */
	@Deprecated
	static int readInt(InputStream is) throws IOException {
		byte[] buf = new byte[Integer.SIZE/8];
		is.read(buf);
		return Util.bytesToInt(buf);
	}

	/**
	 * Read an long from an InputStream.
	 * @deprecated Array allocation causes GC pressure
	 */
	@Deprecated
	static long readLong(InputStream is) throws IOException {
		byte[] buf = new byte[Long.SIZE/8];
		is.read(buf);
		return Util.bytesToLong(buf);
	}
	
	/**
	 * From a list of ByteBuffers, return a list of ByteBuffers spanning the
	 * first nBytes bytes. The positions of the ByteBuffers in the "from" list
	 * will advance past the bytes that have been "read."
	 */
	static List<ByteBuffer> extractBufs(int nBytes, List<ByteBuffer> fromBufs) 
	throws IOException {
		LinkedList<ByteBuffer> outBufs = new LinkedList<ByteBuffer>();
		for(ByteBuffer bb: fromBufs) {
			int remainingThisBuf = bb.remaining();
			if(remainingThisBuf == 0) {
				continue;
			}
			int nBytesThisBuffer = Math.min(nBytes, remainingThisBuf);
			ByteBuffer newBb = bb.slice();
			outBufs.add(newBb);
			newBb.limit(newBb.position() + nBytesThisBuffer);
			bb.position(bb.position() + nBytesThisBuffer);
			nBytes -= nBytesThisBuffer;
			if(nBytes == 0) {
				return outBufs;
			}
		}
		throw new EOFException("Not enough bytes to extract");
	}
	
	/**
	 * From a list of ByteBuffers, return an array of bytes containing the first
	 * n bytes. The positions of the ByteBuffers in the "from" list will advance 
	 * past the bytes that have been "read."
	 */
	public static byte[] extractBytes(int nBytes, List<ByteBuffer> fromBufs) 
	throws IOException {
		byte[] outArr = new byte[nBytes];
		int outIndex = 0;
		for(ByteBuffer bb: fromBufs) {
			int nBytesThisBuffer = Math.min(nBytes, bb.remaining());
			bb.get(outArr, outIndex, nBytesThisBuffer);
			nBytes -= nBytesThisBuffer;
			if(nBytes == 0) {
				return outArr;
			}
			outIndex += nBytesThisBuffer;
		}
		throw new EOFException("Not enough bytes to extract");
	}

	public static long extractLong(List<ByteBuffer> fromBufs) throws IOException {
		byte[] bytes = extractBytes(Long.SIZE/8, fromBufs);
		return Util.bytesToLong(bytes);
	}

	public static int extractInt(List<ByteBuffer> fromBufs) throws IOException {
		byte[] bytes = extractBytes(Integer.SIZE/8, fromBufs);
		return Util.bytesToInt(bytes);
	}
	
	public static byte extractByte(List<ByteBuffer> fromBufs) throws IOException {
		for(ByteBuffer bb: fromBufs) {
			if(bb.remaining() > 0) {
				return bb.get();
			}
		}
		throw new EOFException();
	}
	
	/**
	 * Given a list of ByteBuffers, return a list of ByteBuffer duplicate()s.
	 */
	public static List<ByteBuffer> duplicateBufferList(List<ByteBuffer> inList) {
		List<ByteBuffer> outList = new LinkedList<ByteBuffer>();
		for(ByteBuffer bb: inList) {
			outList.add(bb.duplicate());
		}
		return outList;
	}
}
