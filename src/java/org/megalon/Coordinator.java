package org.megalon;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.avro.AvroValidate;
import org.megalon.avro.AvroValidateResp;
import org.megalon.messages.MegalonMsg;
import org.megalon.messages.MsgValidate;
import org.megalon.messages.MsgValidateResp;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.NextAction;
import org.megalon.multistageserver.MultiStageServer.Stage;
import org.megalon.multistageserver.SelectorStage;
import org.megalon.multistageserver.SocketAccepter;


public class Coordinator {
	static Log logger = LogFactory.getLog(Coordinator.class);
	Megalon megalon;
	
	MultiStageServer<MPayload> coreServer;
	SocketAccepter<MSocketPayload> socketAccepter = 
		new SocketAccepter<MSocketPayload>();
	MultiStageServer<MSocketPayload> socketServer;
	boolean ready = false;
	AvroRpcEncode encodeStage;
	AvroRpcDecode decodeStage;
	SelectorStage<MSocketPayload> selectorStage;
	
	CoordExecStage execStage;
	
	public Coordinator(Megalon megalon) {
		this.megalon = megalon;
	}

	public void init() throws Exception {
		logger.debug("Coordinator init'ing");
		
		// The coreServer contains the stages that are the same regardless of
		// whether the request arrived by socket or by function call.
		Set<Stage<MPayload>> coreStages = new HashSet<Stage<MPayload>>();
		coreStages.add(execStage);
		coreServer = new MultiStageServer<MPayload>(coreStages);
		
		// Set up the mapping from megalon message types to avro message types.
		// This is used to encode the core server's result in Avro.
		Map<Class<? extends MegalonMsg>,Class<? extends SpecificRecordBase>> encoderClassMap = 
			new HashMap<Class<? extends MegalonMsg>,Class<? extends SpecificRecordBase>>();
		encoderClassMap.put(MsgValidateResp.class, AvroValidateResp.class);
		
		// Set up the mapping from avro message types to megalon message types.
		// This is used to decode the request from Avro into a megalon message.
		Map<Byte,Class<? extends SpecificRecordBase>> msgTypes = 
			new HashMap<Byte,Class<? extends SpecificRecordBase>>();
		msgTypes.put(MsgValidate.MSG_ID, AvroValidate.class);
		
		// Set up the mapping from avro message types to megalon message types.
		// This is used to decode the incoming request for the core server
		Map<Class<? extends SpecificRecordBase>,Class<? extends MegalonMsg>> decoderClassMap = 
			new HashMap<Class<? extends SpecificRecordBase>,Class<? extends MegalonMsg>>();
		decoderClassMap.put(AvroValidate.class, MsgValidate.class);
		
		// The socketServer contains the stages that are only executed by 
		// incoming RPC request (not local function calls).
		Set<Stage<MSocketPayload>> socketSvrStages = 
			new HashSet<Stage<MSocketPayload>>();
		decodeStage = new AvroRpcDecode();
		selectorStage = new SelectorStage<MSocketPayload>(decodeStage, 
				"coordSelectorStage", 10, 10);
		encodeStage = new AvroRpcEncode(selectorStage, encoderClassMap, 10, 10);
		decodeStage.init(msgTypes, decoderClassMap, coreServer, execStage, 
				selectorStage, encodeStage, 10, 10);
		socketSvrStages.add(selectorStage);
		socketSvrStages.add(encodeStage);
		socketSvrStages.add(decodeStage);
		socketServer = new MultiStageServer<MSocketPayload>(socketSvrStages);
		
	}
	
	public void startSocketAccepter() {
		socketAccepter.init(socketServer, null, megalon.config.coord_port,
				selectorStage, new MSocketPayload.Factory(), false);
		Thread accepterThread = new Thread() {
			public void run() {
				try {
					logger.debug("Coordinator socket accepter thread starting");
					socketAccepter.runForever();
				} catch (Exception e) {
					logger.error("Coordinator accepter exception: " + e);
				}
			}
		};
		accepterThread.setDaemon(true);
		accepterThread.start();
	}
	
//	public static class MCoordPayload extends MPayload {
//		byte msgType;
//		MegalonMsg req;
//		MegalonMsg resp;
//		
//		/**
//		 * Constructor used for local (same JVM) requests to the coordinator.
//		 */
//		public MCoordPayload(byte msgType) {
//			this.msgType = msgType;
//		}
//		
//		/**
//		 * Constructor used by remote requests that enter through the message
//		 * decoder stage (from outside this JVM). These requests will have a 
//		 * MSocketPayload associated with them, which is an argument to this 
//		 * constructor.
//		 */
//		public MCoordPayload(byte msgType, Payload outerPayload) {
//			super(outerPayload);
//			this.msgType = msgType;
//		}
//	}
	
	public static class CoordExecStage implements Stage<MPayload> {
		public NextAction<MPayload> runStage(MPayload payload)
				throws Exception {
			return null;
		}

		public int getNumConcurrent() {
			return 1;
		}

		public String getName() {
			return CoordExecStage.class.getCanonicalName();
		}

		public int getBacklogSize() {
			return 10;
		}

		public void setServer(MultiStageServer<MPayload> server) {}
	}
}
