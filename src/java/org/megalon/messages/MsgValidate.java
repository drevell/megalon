package org.megalon.messages;

import java.nio.ByteBuffer;

import org.megalon.avro.AvroValidate;

/**
 * A message to the Coordinator regarding the state of a particular entity group
 * in its local replica. 
 */
public class MsgValidate extends MegalonMsg {
	public static final byte MSG_ID = 5;
	
	public final byte[] entityGroup;
	public final long walIndex;
	public final boolean isValid;
	
	public MsgValidate(AvroValidate avroValidate) {
		super(MSG_ID);
		this.walIndex = avroValidate.walIndex;
		this.isValid = avroValidate.isValid;
		
		assert avroValidate.entityGroup.remaining() == avroValidate.entityGroup.capacity();
		this.entityGroup = avroValidate.entityGroup.array();
//		this.entityGroup = new byte[avroValidate.entityGroup.remaining()];
//		avroValidate.entityGroup.get(this.entityGroup);
	}
	
	public MsgValidate(byte[] entityGroup, long walIndex, boolean isValid) {
		super(MSG_ID);
		this.entityGroup = entityGroup;
		this.walIndex = walIndex;
		this.isValid = isValid;
	}
	
	public AvroValidate toAvro() {
		AvroValidate avroValidate = new AvroValidate();
		avroValidate.entityGroup = ByteBuffer.wrap(entityGroup);
		avroValidate.walIndex = walIndex;
		avroValidate.isValid = isValid;
		return avroValidate;
	}
}
