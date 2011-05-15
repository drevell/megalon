package org.megalon.messages;

import org.megalon.avro.AvroAcceptResponse;

public class MsgAcceptResp extends MegalonMsg {
	public static final byte MSG_ID = 4;
	
	boolean acked;
	
	public MsgAcceptResp(boolean acked) {
		super(MSG_ID);
		this.acked = acked;
	}
	
	public AvroAcceptResponse toAvro() {
		AvroAcceptResponse resp = new AvroAcceptResponse();
		resp.acked = acked;
		return resp;
	}
}
