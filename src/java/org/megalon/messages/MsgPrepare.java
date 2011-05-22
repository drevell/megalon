package org.megalon.messages;

import java.nio.ByteBuffer;

import org.megalon.avro.AvroPrepare;

public class MsgPrepare extends MegalonMsg {
	public static final byte MSG_ID = 1;
	
	public final long n;
	public final long walIndex;
	public final byte[] entityGroup;
	
	public MsgPrepare(AvroPrepare avroPrepare) {
		super(MSG_ID);
		this.n = avroPrepare.n;
		this.walIndex = avroPrepare.walIndex;
	
		assert avroPrepare.entityGroup.remaining() == avroPrepare.entityGroup.capacity();
		this.entityGroup = avroPrepare.entityGroup.array();
//		this.entityGroup = new byte[avroPrepare.entityGroup.remaining()];
//		avroPrepare.entityGroup.get(this.entityGroup);
	}
	
	public AvroPrepare toAvro() {
		AvroPrepare avroPrep = new AvroPrepare();
		avroPrep.n = n;
		avroPrep.walIndex = walIndex;
		avroPrep.entityGroup = ByteBuffer.wrap(entityGroup);
		return avroPrep;
	}
}
