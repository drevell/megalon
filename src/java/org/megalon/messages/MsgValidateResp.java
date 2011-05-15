package org.megalon.messages;

import org.megalon.avro.AvroValidateResp;

public class MsgValidateResp extends MegalonMsg {
	public static final byte MSG_ID = 5;
	
	public boolean acked;
	
	public MsgValidateResp(boolean acked) {
		super(MSG_ID);
		this.acked = acked;
	}
	
	public AvroValidateResp toAvro() {
		AvroValidateResp avroResp = new AvroValidateResp();
		avroResp.acked = acked;
		return avroResp;
	}
}
