package org.megalon;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.messages.MegalonMsg;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.Finisher;
import org.megalon.multistageserver.MultiStageServer.NextAction;
import org.megalon.multistageserver.MultiStageServer.NextAction.Action;
import org.megalon.multistageserver.MultiStageServer.Stage;

/**
 * This is a stage that can be used as part of a MultiStageServer. It will take
 * the incoming socket data and interpret it as an RPC request in the form
 * (length.4) (rpcSerial.8) (msgType.1) (payload.X). Once the request has been
 * parsed, it 
 * 
 */
public class AvroRpcDecode implements Stage<MSocketPayload>, Finisher<MPayload> {
	public static final int MAX_MSG_LEN = 10000000; 
	
	Log logger = LogFactory.getLog(AvroRpcDecode.class);
	Map<Byte,Class<? extends SpecificRecordBase>> msgTypes;
	Map<Class<? extends SpecificRecordBase>,Class<? extends MegalonMsg>> megalonClasses; 
	MultiStageServer<MPayload> coreServer; 
	Stage<MPayload> coreStage;
	Stage<MSocketPayload> selectorStage;
	int numConcurrent;
	int backlog;
	MultiStageServer<MSocketPayload> myServer;
	AvroRpcEncode avroEncode;
	volatile boolean inited = false;
	
	public void init(Map<Byte,Class<? extends SpecificRecordBase>> msgTypes,
	Map<Class<? extends SpecificRecordBase>,Class<? extends MegalonMsg>> megalonClasses,
	MultiStageServer<MPayload> coreServer, Stage<MPayload> coreStage,
	Stage<MSocketPayload> selectorStage, AvroRpcEncode avroEncode,
	int numConcurrent, int backlog) {
		this.msgTypes = msgTypes;
		this.megalonClasses = megalonClasses;
		this.selectorStage = selectorStage;
		this.avroEncode = avroEncode;
		this.numConcurrent = numConcurrent;
		this.backlog = backlog;
		this.coreServer = coreServer;
		this.coreStage = coreStage;
		inited = true;
	}
	
	public NextAction<MSocketPayload> runStage(MSocketPayload payload) throws
	IOException {
		logger.debug("In AvroRpcDecode.runStage");
		if(!inited) {
			logger.error("AvroRpcDecode run before init?!?");
			return new NextAction<MSocketPayload>(Action.FINISHED, null);
		}
		logger.debug("Incoming buffers: " + RPCUtil.strBufs(payload.readBufs));
		while(true) {
			try {
				if(!RPCUtil.hasCompleteMessage(payload.readBufs)) {
					payload.continueReading = true;
					logger.debug("Don't have complete msg, back to selector");
					return new NextAction<MSocketPayload>(Action.FORWARD, 
						selectorStage);
				}
			} catch (IOException e) {
				logger.warn("Misformatted message", e);
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
			}
			logger.debug("Have >0 complete messages");
			
			// Read the incoming msg length prefix, and sanity check it
			int msgLen = RPCUtil.extractInt(payload.readBufs);
			logger.debug("Incoming msgLen " + msgLen);
			int minReqdBytes = RPCUtil.RPC_HEADER_SIZE - 4;
			if (msgLen < minReqdBytes) {
				logger.warn("Message was too short to contain "
						+ "required fields, need " + minReqdBytes);
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
			}
			if (msgLen > MAX_MSG_LEN) {
				// The incoming message claims to be very large. It's
				// probably just misformatted, or the wrong protocol.
				logger.warn("ReplServer msg claimed to be huge. Closing.");
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
			}
			
			// Extract unique RPC request ID
			payload.rpcSerial = RPCUtil.extractLong(payload.readBufs);
			logger.debug("Incoming request serial is " + payload.rpcSerial);
			
			// Extract message body
			List<ByteBuffer> msg = RPCUtil.extractBufs(msgLen-Long.SIZE/8, 
					payload.readBufs);
			logger.debug("Extracted msg is: " + RPCUtil.strBufs(msg));

			byte msgType = RPCUtil.extractByte(msg);
			ByteBufferInputStream msgIs = new ByteBufferInputStream(msg);
			Decoder dec = 
				DecoderFactory.get().binaryDecoder(msgIs, null);
			
			// Create an avro Reader of the correct type for this message, and
			// read the incoming Avro object.
			// TODO this could be more efficient wrt num of allocations
			Class<? extends SpecificRecordBase> avroClass = msgTypes.get(msgType);
			if(avroClass == null) {
				logger.warn("Repl server saw unexpected message type: " +
						msgType);
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
			}
			DatumReader reader = new SpecificDatumReader(avroClass);
			Object avroMsg = reader.read(null, dec);
			
			// Convert the Avro object to a MegalonMsg object, which is the input
			// to the core server.
			Class<? extends MegalonMsg> megalonClass = megalonClasses.get(avroClass);
			if(megalonClass == null) {
				logger.error("No Megalon class for avro class: " + avroClass);
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
			}
			Constructor ctor;
			MegalonMsg req;
			try {
				ctor = megalonClass.getConstructor(new Class[] {avroClass});
				req = (MegalonMsg)ctor.newInstance(avroMsg);
			} catch (NoSuchMethodException e) {
				logger.error("Megalon class lacks the right constructor: " + 
						megalonClass, e);
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
			} catch (InvocationTargetException e) {
				logger.warn("Megalon msg constructor threw exception", e);
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
			} catch (IllegalAccessException e) {
				logger.warn("Illegal access to megalon msg ctor", e);
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
			} catch (InstantiationException e) {
				logger.error("Can't instantiate abstract class", e);
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
			}
			MPayload newPayload = new MPayload(msgType, req, payload);
			coreServer.enqueue(newPayload, coreStage, this);
			
			// Discard ByteBuffers that we have read completely
			while (!payload.readBufs.isEmpty()
					&& payload.readBufs.getFirst().remaining() == 0) {
				payload.readBufs.removeFirst();
			}
		}
	}
		
	/**
	 * This finisher will handle payloads when the main replication server is
	 * done with them.
	 */
	public void finish(MPayload mPayload) {
		MSocketPayload mSockPayload = (MSocketPayload)mPayload.getOuterPayload();
		mSockPayload.setResponse(mPayload.resp);
		myServer.enqueue(mSockPayload, avroEncode, mSockPayload.finisher);
	}

	public int getNumConcurrent() {
		return numConcurrent;
	}

	public String getName() {
		return this.getClass().getCanonicalName();
	}

	public int getBacklogSize() {
		return backlog;
	}

	public void setServer(MultiStageServer<MSocketPayload> server) {
		myServer = server;
	}
}
