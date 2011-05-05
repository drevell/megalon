package org.megalon.messages;

import org.megalon.avro.AvroAcceptResponse;

public class MsgAcceptResp implements MegalonMsg {
	boolean acked;
	
	public MsgAcceptResp(boolean acked) {
		this.acked = acked;
	}
	
	public AvroAcceptResponse toAvro() {
		AvroAcceptResponse resp = new AvroAcceptResponse();
		resp.acked = acked;
		return resp;
	}
}
