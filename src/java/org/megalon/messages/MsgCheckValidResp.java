package org.megalon.messages;

import org.megalon.avro.AvroCheckValidResp;

public class MsgCheckValidResp extends MegalonMsg {
	public static final byte MSG_ID = 8;
	public boolean isValid;
	
	public AvroCheckValidResp toAvro() {
		// TODO Auto-generated method stub
		AvroCheckValidResp avro = new AvroCheckValidResp();
		avro.isValid = isValid;
		return avro;
	}
	
	public MsgCheckValidResp(boolean isValid) {
		super(MSG_ID);
		this.isValid = isValid; 
	}
	
	public MsgCheckValidResp(AvroCheckValidResp avro) {
		super(MSG_ID);
		this.isValid = avro.isValid;
	}
}
