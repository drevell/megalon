package org.megalon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.messages.MegalonMsg;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.NextAction;
import org.megalon.multistageserver.MultiStageServer.NextAction.Action;
import org.megalon.multistageserver.MultiStageServer.Stage;
import org.megalon.multistageserver.SelectorStage;

public class AvroRpcEncode implements Stage<MSocketPayload> {
	Log logger = LogFactory.getLog(AvroRpcEncode.class);
	
	MultiStageServer<MSocketPayload> myServer;
	Map<Class<? extends MegalonMsg>,Class<? extends SpecificRecordBase>> msgClasses;
	SelectorStage<MSocketPayload> selectorStage;
	int numConcurrent;
	int backlog;
	
	public AvroRpcEncode(SelectorStage<MSocketPayload> selectorStage,
	Map<Class<? extends MegalonMsg>,Class<? extends SpecificRecordBase>> msgClasses,
	int numConcurrent, int backlog) {
		this.selectorStage = selectorStage;
		this.msgClasses = msgClasses;
		this.numConcurrent = numConcurrent;
		this.backlog = backlog;
	}
	
	public NextAction<MSocketPayload> runStage(MSocketPayload payload) {
		Class<? extends MegalonMsg> megalonClass = payload.resp.getClass();
		Class<? extends SpecificRecordBase> avroClass = msgClasses.get(megalonClass);
		if(avroClass == null) {
			logger.error("No avro class for megalon msg type: " + 
					megalonClass.getCanonicalName());
			return new NextAction<MSocketPayload>(Action.FINISHED, null);
		}
		
		try {
			// TODO should reuse writers and encoders
			DatumWriter avroWriter = new SpecificDatumWriter(avroClass);
			SpecificRecordBase avroObj = payload.resp.toAvro();
			ByteBufferOutputStream os = payload.getOutputStream();
			os.write(payload.resp.getMsgId());
			Encoder enc = EncoderFactory.get().binaryEncoder(os, null);
			avroWriter.write(avroObj, enc);
			enc.flush();
			os.flush();
			
			// Output format: nBytes, rpcSerial, body
			int numOutBytes = Long.SIZE/8;
			List<ByteBuffer> bbList = os.getBufferList();
			logger.debug("Encoded response: " + 
					RPCUtil.strBufs(bbList));
			for(ByteBuffer bb: bbList ) {
				numOutBytes += bb.remaining();
			}
			
			logger.debug("Writing buffer length: " + numOutBytes);
			os.write(Util.intToBytes(numOutBytes));
			logger.debug("Writing serial: " + payload.rpcSerial);
			os.write(Util.longToBytes(payload.rpcSerial));
			os.append(bbList); // efficient no-copy append
			os.flush();
			return new NextAction<MSocketPayload>(Action.FORWARD, selectorStage);
		} catch (IOException e) {
			logger.warn("IOException encoding to avro", e);
			return new NextAction<MSocketPayload>(Action.FINISHED, null);
		}
		
		
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
		this.myServer = server;
	}
}
