package org.megalon.messages;

import org.megalon.avro.AvroAcceptResponse;

public class MsgAcceptResp implements MegalonMsg {
//	long reqSerial;
	boolean acked;
	
	public MsgAcceptResp(boolean acked) {
//		this.reqSerial = reqSerial;
		this.acked = acked;
	}
	
	public AvroAcceptResponse toAvro() {
		AvroAcceptResponse resp = new AvroAcceptResponse();
//		resp.reqSerial = reqSerial;
		resp.acked = acked;
		return resp;
	}
}
