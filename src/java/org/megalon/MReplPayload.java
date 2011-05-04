package org.megalon;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.megalon.Config.Host;
import org.megalon.messages.MegalonMsg;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.Stage;
import org.megalon.multistageserver.Payload;

public class MReplPayload extends Payload {
	byte msgType;
	MegalonMsg req;
	MegalonMsg resp;
	long reqSerial;
	
	public MReplPayload(byte msgType) {
		this(msgType, null);
	}
	
	public MReplPayload(byte msgType, Payload outerPayload) {
		super(outerPayload);
		this.msgType = msgType;
	}
	
	
//	public static class Factory implements PayloadFactory<MPayload> {
//		public MPayload makePayload(SocketChannel sockChan) {
//			return new MPayload();
//		}
//	}
	
	
}
