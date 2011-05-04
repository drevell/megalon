package org.megalon;

import java.io.IOException;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.avro.AvroAccept;
import org.megalon.avro.AvroPrepare;
import org.megalon.messages.MegalonMsg;
import org.megalon.messages.MsgAccept;
import org.megalon.messages.MsgPrepare;
import org.megalon.multistageserver.BBInputStream;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.Finisher;
import org.megalon.multistageserver.MultiStageServer.NextAction;
import org.megalon.multistageserver.MultiStageServer.NextAction.Action;
import org.megalon.multistageserver.MultiStageServer.Stage;

/**
 * This stage does two things:
 *  1. Sockets that just received an RPC request from a remote node
 *     will be sent here to have the request decoded from Avro into a Java
 *     object, which will be sent on to the ReplServer execution stage.
 *  2. Request payloads that arrived via a socket that have finished 
 *     processing by the ReplServer execution stage will be sent to this 
 *     stage to have the reply encoded into Avro. The encoded payload will
 *     then be sent to the socketServer to send the encoded response back
 *     to the client.
 */
class AvroDecodeStage implements MultiStageServer.Stage<MSocketPayload>, 
Finisher<MReplPayload> {
	public static final int MAX_MSG_LEN = 10000000; 
	
	MultiStageServer<MSocketPayload> myServer;
	Stage<MSocketPayload> myServerNextStage;
	MultiStageServer<MReplPayload> enqueueServer; 
	Stage<MReplPayload> enqueueStage;
	Stage<MSocketPayload> selectorStage;
	
	Log logger = LogFactory.getLog(AvroDecodeStage.class);
	
	/**
	 * No-arg constructor, the caller must call init() before using this object.
	 */
	public AvroDecodeStage() {}
	
	public AvroDecodeStage(Stage<MSocketPayload> myServerNextStage,
			MultiStageServer<MReplPayload> enqueueServer,  
			Stage<MReplPayload> entryStage,
			Stage<MSocketPayload> selectorStage) {
		this.init(myServerNextStage, enqueueServer, entryStage, selectorStage);
	}
	
	public void init(Stage<MSocketPayload> myServerNextStage,
			MultiStageServer<MReplPayload> enqueueServer,  
			Stage<MReplPayload> entryStage,
			Stage<MSocketPayload> selectorStage) {
		this.enqueueServer = enqueueServer;
		this.myServerNextStage = myServerNextStage;
		this.enqueueStage = entryStage;
		this.selectorStage = selectorStage;
	}
	
	final DatumReader<AvroPrepare> prepareReader = 
		new SpecificDatumReader<AvroPrepare>(AvroPrepare.class);
	final DatumReader<AvroAccept> acceptReader = 
		new SpecificDatumReader<AvroAccept>(AvroAccept.class);
	
	/**
	 * This finisher will handle payloads when the main replication server is
	 * done with them.
	 */
	public void finish(MReplPayload mPayload) {
		MSocketPayload mSockPayload = (MSocketPayload)mPayload.getOuterPayload();
		mSockPayload.resp = mPayload.resp;
		mSockPayload.reqType = mPayload.msgType;
		myServer.enqueue(mSockPayload, myServerNextStage);
	}
	
	public NextAction<MSocketPayload> runStage(MSocketPayload payload) 
	throws Exception {
		// TODO it would be nice if we didn't use Avro directly here. It
		// would be better to have some general system where any
		// serialization system could be plugged in.
		
		logger.debug("In AvroDecodeStage.runStage");
		logger.debug("Incoming buffers: " + RPCUtil.strBufs(payload.readBufs));
		while(true) {
			try {
				if(!RPCUtil.hasCompleteMessage(payload.readBufs)) {
					logger.debug("Don't have complete msg, back to selector");
					return new NextAction<MSocketPayload>(Action.FORWARD, 
						selectorStage);
				}
			} catch (IOException e) {
				logger.warn("Misformatted message", e);
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
			}
			logger.debug("Have >0 complete messages");
			BBInputStream is = payload.is;

			// Read the incoming msg length prefix, and sanity check it
			int msgLen = RPCUtil.readInt(is);
			logger.debug("Incoming msgLen " + msgLen);
			int minReqdBytes = RPCUtil.RPC_HEADER_SIZE-4; 
			if(msgLen < minReqdBytes) {
				logger.warn("Message was too short to contain " +
						"required fields, need " + minReqdBytes);
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
			}
			if(msgLen > MAX_MSG_LEN) {
				// The incoming message claims to be very large. It's
				// probably just misformatted, or the wrong protocol.
				logger.warn("ReplServer msg claimed to be huge. Closing.");
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
			}
			assert is.available() >= msgLen;

			long reqSerial = RPCUtil.readLong(is);

			byte[] msgType = new byte[1];
			is.read(msgType);
			Decoder dec = 
				DecoderFactory.get().binaryDecoder(payload.is, null);
			MReplPayload newPayload = new MReplPayload(msgType[0], payload);
			newPayload.reqSerial = reqSerial;
			
			switch(msgType[0]) {
			case MegalonMsg.MSG_PREPARE:
				// TODO reuse avro obj here?
				AvroPrepare avroPrepare = prepareReader.read(null, dec);
				newPayload.req = new MsgPrepare(avroPrepare);
				logger.debug("DecodeStage Enqueueing prepare into ReplServer");
				enqueueServer.enqueue(newPayload, enqueueStage, this);
				logger.debug("AvroDecode has a \"prepare\"");
				return new NextAction<MSocketPayload>(Action.IGNORE, null);
			case MegalonMsg.MSG_ACCEPT:
				AvroAccept avroAccept = acceptReader.read(null, dec);
				newPayload.req = new MsgAccept(avroAccept);
				logger.debug("DecodeStage Enqueueing accept into ReplServer");
				enqueueServer.enqueue(newPayload, enqueueStage, this);
				return new NextAction<MSocketPayload>(Action.IGNORE, null);
			default:
				logger.warn("Repl server saw unexpected message type: " +
						msgType[0]);
				return new NextAction<MSocketPayload>(Action.FINISHED, null);
				
			}
		}
	}

	public int getNumConcurrent() {
		return 10; // TODO configurable
	}

	public String getName() {
		return this.getClass().getName();
	}

	public int getBacklogSize() {
		return 100; // TODO configurable
	}

	public void setServer(MultiStageServer<MSocketPayload> server) {
		this.myServer = server;
	}
}