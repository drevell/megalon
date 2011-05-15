//package org.megalon;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.util.List;
//
//import org.apache.avro.io.DatumWriter;
//import org.apache.avro.io.Encoder;
//import org.apache.avro.io.EncoderFactory;
//import org.apache.avro.specific.SpecificDatumWriter;
//import org.apache.avro.util.ByteBufferOutputStream;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.megalon.avro.AvroAcceptResponse;
//import org.megalon.avro.AvroPrepareResponse;
//import org.megalon.messages.MegalonMsg;
//import org.megalon.messages.MsgAcceptResp;
//import org.megalon.messages.MsgPrepareResp;
//import org.megalon.multistageserver.MultiStageServer;
//import org.megalon.multistageserver.MultiStageServer.NextAction;
//import org.megalon.multistageserver.MultiStageServer.NextAction.Action;
//import org.megalon.multistageserver.MultiStageServer.Stage;
//
//class AvroEncodeStage implements MultiStageServer.Stage<MSocketPayload> {
//	Log logger = LogFactory.getLog(AvroEncodeStage.class);
//	Stage<MSocketPayload> nextStage;
//	MultiStageServer<MReplPayload> server;
//
//	/**
//	 * No-arg constructor, the caller must call init() before using this object.
//	 */
//	public AvroEncodeStage() {}
//	
//	public AvroEncodeStage(Stage<MSocketPayload> nextStage) {
//		this.init(nextStage);
//	}
//	
//	public void init(Stage<MSocketPayload> nextStage) {
//		this.nextStage = nextStage;
//	}
//	
//	public NextAction<MSocketPayload> runStage(MSocketPayload mSockPayload) 
//	throws Exception {
//		DatumWriter<AvroPrepareResponse> prepareRespWriter = new 
//			SpecificDatumWriter<AvroPrepareResponse>(AvroPrepareResponse.class);
//		DatumWriter<AvroAcceptResponse> acceptRespWriter = 
//			new SpecificDatumWriter<AvroAcceptResponse>(AvroAcceptResponse.class);
//
//		ByteBufferOutputStream os = mSockPayload.getOutputStream();
//		Encoder enc = EncoderFactory.get().binaryEncoder(os, null);
//		logger.debug("AvroEncoder running");
//		if(mSockPayload.resp == null) {
//			logger.debug("Null response in payload");
//		} else {
//			try {
//				os.reset(); // Make sure output buffer is empty
//				boolean haveResponse = false;
//				switch(mSockPayload.reqType) {
//				case MegalonMsg.MSG_PREPARE:
//					logger.debug("Encoding prepare-response: " + mSockPayload.resp);
//					os.write(MegalonMsg.MSG_PREPARE_RESP);
//					AvroPrepareResponse avroPrepResp = 
//						((MsgPrepareResp)mSockPayload.resp).toAvro();
//					prepareRespWriter.write(avroPrepResp, enc);
//					haveResponse = true;
//					break;
//				case MegalonMsg.MSG_ACCEPT:
//					os.write(MegalonMsg.MSG_ACCEPT_RESP);
//					AvroAcceptResponse avroAccResp =
//						((MsgAcceptResp)(mSockPayload.resp)).toAvro();
//					logger.debug("Encoding accept-response: " + avroAccResp.acked);
//					acceptRespWriter.write(avroAccResp, enc);
//					haveResponse = true;
//					break;
//				default:
//					logger.warn("Unknown msg type, can't encode avro: " +
//							mSockPayload.reqType);
//					
//				}
//				if(haveResponse) {
//					enc.flush();
//					os.flush();
//
//					// Output format: nBytes, rpcSerial, body
//					int numOutBytes = Long.SIZE/8;
//					List<ByteBuffer> bbList = os.getBufferList();
//					logger.debug("Encoded response: " + 
//							RPCUtil.strBufs(bbList));
//					for(ByteBuffer bb: bbList ) {
//						numOutBytes += bb.remaining();
//					}
//					
//					logger.debug("Writing buffer length: " + numOutBytes);
//					os.write(Util.intToBytes(numOutBytes));
//					logger.debug("Writing serial: " + mSockPayload.rpcSerial);
//					os.write(Util.longToBytes(mSockPayload.rpcSerial));
//					os.append(bbList); // efficient no-copy append
//					os.flush();
//					
//					return new NextAction<MSocketPayload>(Action.FORWARD, nextStage);
//				}
//			} catch (IOException e) {
//				logger.warn("IOException writing Avro to buffer", e);
//			} catch (Exception e) {
//				logger.warn("Other exception", e);
//			}
//		}
//		return new NextAction<MSocketPayload>(Action.FINISHED, null);
//	}
//
//	public int getNumConcurrent() {
//		return 3;
//	}
//
//	public String getName() {
//		return this.getClass().getName();
//	}
//
//	public int getBacklogSize() {
//		return 10;
//	}
//
//	public void setServer(MultiStageServer<MSocketPayload> server) {}
//}