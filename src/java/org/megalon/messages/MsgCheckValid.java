package org.megalon.messages;

import java.nio.ByteBuffer;

import org.megalon.avro.AvroCheckValid;

public class MsgCheckValid extends MegalonMsg {
	public static final byte MSG_ID = 7;
	public byte[] entityGroup;
	
	public MsgCheckValid(byte[] entityGroup) {
		super(MSG_ID);
		this.entityGroup = entityGroup;
	}
	
	public MsgCheckValid(AvroCheckValid avro) {
		super(MSG_ID);
		entityGroup = new byte[avro.entityGroup.remaining()];
		avro.entityGroup.get(entityGroup);
	}
	
	public AvroCheckValid toAvro() {
		AvroCheckValid avro = new AvroCheckValid();
		avro.entityGroup = ByteBuffer.wrap(entityGroup);
		return avro;
	}
}
