package org.megalon;

import org.megalon.messages.MegalonMsg;
import org.megalon.multistageserver.Payload;

public class MReplPayload extends Payload {
	byte msgType;
	MegalonMsg req;
	MegalonMsg resp;
	
	public MReplPayload(byte msgType) {
		this(msgType, null);
	}
	
	public MReplPayload(byte msgType, Payload outerPayload) {
		super(outerPayload);
		this.msgType = msgType;
	}
}
