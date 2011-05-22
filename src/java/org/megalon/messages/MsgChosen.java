package org.megalon.messages;

import java.nio.ByteBuffer;

import org.megalon.avro.AvroChosen;

public class MsgChosen extends MegalonMsg {
	public static final byte MSG_ID = 9;
	public byte[] entityGroup;
	public long walIndex;
	
	public MsgChosen(AvroChosen avro) {
		super(MSG_ID);
		assert avro.entityGroup.remaining() == avro.entityGroup.capacity();
		this.entityGroup = avro.entityGroup.array();
		this.walIndex = avro.walIndex;
	}

	public AvroChosen toAvro() {
		AvroChosen avro = new AvroChosen();
		avro.entityGroup = ByteBuffer.wrap(entityGroup);
		avro.walIndex = walIndex;
		return avro;
	}
	
	public MsgChosen(byte[] entityGroup, long walIndex) {
		super(MSG_ID);
		this.entityGroup = entityGroup;
		this.walIndex = walIndex;
	}
}
