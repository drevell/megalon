package org.megalon;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config.Host;
import org.megalon.Config.ReplicaDesc;
import org.megalon.avro.AvroAccept;
import org.megalon.avro.AvroAcceptResponse;
import org.megalon.avro.AvroCheckValid;
import org.megalon.avro.AvroCheckValidResp;
import org.megalon.avro.AvroChosen;
import org.megalon.avro.AvroPrepare;
import org.megalon.avro.AvroPrepareResponse;
import org.megalon.avro.AvroValidate;
import org.megalon.avro.AvroValidateResp;
import org.megalon.messages.MegalonMsg;
import org.megalon.messages.MsgAccept;
import org.megalon.messages.MsgAcceptResp;
import org.megalon.messages.MsgCheckValid;
import org.megalon.messages.MsgCheckValidResp;
import org.megalon.messages.MsgChosen;
import org.megalon.messages.MsgPrepare;
import org.megalon.messages.MsgPrepareResp;
import org.megalon.messages.MsgValidate;
import org.megalon.messages.MsgValidateResp;

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
				//logger.debug("Incoming msg length is: " + msgLengthField);
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
	
	/**
	 * Send the given list of ByteBuffers to all replicas. Returns the
	 * number of replicas for which we don't have an open socket, which
	 * are failures. Other (reachable) replicas may still fail later.
	 */
	static int sendToRemoteReplSvrs(Megalon megalon, Collection<ReplicaDesc> replicas, 
			List<ByteBuffer> outBytes, MPaxPayload payload) {
		int numFailedReplicas = 0;
		List<ByteBuffer> outBytesThisRepl;
		for(ReplicaDesc replicaDesc: replicas) {
			if(replicaDesc == megalon.config.myReplica) {
				// We assume the local replica was handled elsewhere.
				continue;
			}
			boolean aHostSucceeded = false;
			for(Host host: (List<Host>)Util.shuffled(replicaDesc.replsrv)) {
				outBytesThisRepl = RPCUtil.duplicateBufferList(outBytes);
				//logger.debug("bytes this repl: " + RPCUtil.strBufs(outBytesThisRepl));
				RPCClient rpcCli = megalon.clientData.getReplSrvSocket(host);
				aHostSucceeded |= rpcCli.write(outBytesThisRepl, payload);
				if(!aHostSucceeded) {
					logger.debug("Host write failed: " + host);
				}
				break;
			}
			if(!aHostSucceeded) {
				numFailedReplicas++;
				logger.debug("All hosts for replica failed: " + replicaDesc);
			}
		}
		return numFailedReplicas;
	}
	
	private static Class<? extends MegalonMsg> mgClsForMsgId(byte msgId) {
		switch(msgId) {
		case MsgPrepare.MSG_ID:
			return MsgPrepare.class;
		case MsgPrepareResp.MSG_ID:
			return MsgPrepareResp.class;
		case MsgChosen.MSG_ID:
			return MsgChosen.class;
		case MsgCheckValid.MSG_ID:
			return MsgCheckValid.class;
		case MsgCheckValidResp.MSG_ID:
			return MsgCheckValidResp.class;
		case MsgValidate.MSG_ID:
			return MsgValidate.class;
		case MsgValidateResp.MSG_ID:
			return MsgValidateResp.class;
		case MsgAccept.MSG_ID:
			return MsgAccept.class;
		case MsgAcceptResp.MSG_ID:
			return MsgAcceptResp.class;
		default:
			assert false: "Unrecognized msg type: " + msgId;
			return null; // unreachable, just makes the type checker be quiet 
		}
	}
	
	private static Class<? extends SpecificRecordBase> avroClsForMsgId(byte msgId) {
		switch(msgId) {
		case MsgPrepare.MSG_ID:
			return AvroPrepare.class;
		case MsgPrepareResp.MSG_ID:
			return AvroPrepareResponse.class;
		case MsgChosen.MSG_ID:
			return AvroChosen.class;
		case MsgCheckValid.MSG_ID:
			return AvroCheckValid.class;
		case MsgCheckValidResp.MSG_ID:
			return AvroCheckValidResp.class;
		case MsgValidate.MSG_ID:
			return AvroValidate.class;
		case MsgValidateResp.MSG_ID:
			return AvroValidateResp.class;
		case MsgAccept.MSG_ID:
			return AvroAccept.class;
		case MsgAcceptResp.MSG_ID:
			return AvroAcceptResponse.class;
		default:
			assert false: "Unrecognized msg type: " + msgId;
			return null; // unreachable, just makes the type checker be quiet 
		}
	}
	
	/**
	 * Given an object that's a subclass of MegalonMsg, write its message ID and
	 * Avro-encoded version to the given stream.
	 */
	public static void writeMsgToStream(OutputStream stream, MegalonMsg msg) {
		SpecificRecordBase avroObj = msg.toAvro();
		byte msgId = msg.getMsgId(); 
		try {
			stream.write(msgId);
			Class<? extends SpecificRecordBase> avroClass = avroClsForMsgId(msgId);
			// TODO pool writers and encoders
			DatumWriter writer = new SpecificDatumWriter(avroClass);
			Encoder enc = EncoderFactory.get().binaryEncoder(stream, null);
			writer.write(avroObj, enc);
			enc.flush();
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}
	
	/**
	 * Given an object that's a subclass of MegalonMsg, encode it as an outgoing
	 * RPC message, returning the result as a list of ByteBuffers. 
	 */
	public static List<ByteBuffer> rpcBbEncode(MegalonMsg msg) {
		ByteBufferOutputStream bbos = new ByteBufferOutputStream();
		writeMsgToStream(bbos, msg);
		return bbos.getBufferList();
	}
	
	/**
	 * Given an object that's a subclass of MegalonMsg, encode it as an outgoing
	 * RPC message, returning the result as a byte array. 
	 */
	public static byte[] rpcBytesEncode(MegalonMsg msg) {
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		writeMsgToStream(bao, msg);
		return bao.toByteArray();
	}
	
	/**
	 * Given a List of ByteBuffers that contain an avro-encoded Megalon RPC
	 * message, return the decoded MegalonMsg that's contained in the buffers.
	 * This function assumes the leading msgId byte is NOT part of the msg 
	 * buffers.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static MegalonMsg rpcDecode(byte msgId, List<ByteBuffer> msg) 
	throws IOException {
		ByteBufferInputStream msgIs = new ByteBufferInputStream(msg);
		Decoder dec = 
			DecoderFactory.get().binaryDecoder(msgIs, null);
		
		// Create an avro Reader of the correct type for this message, and
		// read the incoming Avro object.
		// TODO this could be more efficient wrt num of allocations
		Class<? extends SpecificRecordBase> avroClass = avroClsForMsgId(msgId);
		if(avroClass == null) {
			logger.warn("Repl server saw unexpected message type: " +
					msgId);
			return null;
		}
		//logger.debug("Reading avro message of expected class: " + avroClass);
		DatumReader reader = new SpecificDatumReader(avroClass);
		Object avroMsg = reader.read(null, dec);
		
		// Convert the Avro object to a MegalonMsg object, which is the input
		// to the core server.
		Class<? extends MegalonMsg> megalonClass = mgClsForMsgId(msgId);
		assert megalonClass != null: "No Megalon class for avro class: " + avroClass;
		
		Constructor<? extends MegalonMsg> ctor;
		try {
			ctor = megalonClass.getConstructor(new Class[] {avroClass});
			return (MegalonMsg)ctor.newInstance(avroMsg);
		} catch (NoSuchMethodException e) {
			throw new IOException("Megalon class lacks the right constructor: " + 
					megalonClass, e);
		} catch (InvocationTargetException e) {
			throw new IOException("Megalon msg constructor threw exception", e);
		} catch (IllegalAccessException e) {
			throw new IOException("Illegal access to megalon msg ctor", e);
		} catch (InstantiationException e) {
			throw new IOException("Can't instantiate abstract class", e);
		}
	}
}
