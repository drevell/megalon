package org.megalon.messages;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.megalon.WALEntry;
import org.megalon.avro.AvroAccept;

public class MsgAccept extends MegalonMsg {
	public static final byte MSG_ID = 3;
	
	public final WALEntry walEntry;
	public final long walIndex;
	public final byte[] entityGroup;
	
	public MsgAccept(AvroAccept avroAccept) throws IOException {
		super(MSG_ID);

		this.walEntry = new WALEntry(avroAccept.walEntry);
		this.walIndex = avroAccept.walIndex;
		
		assert avroAccept.entityGroup.remaining() == avroAccept.entityGroup.capacity();
		this.entityGroup = avroAccept.entityGroup.array();
//		this.entityGroup = new byte[avroAccept.entityGroup.remaining()];
//		avroAccept.entityGroup.get(this.entityGroup);
	}
	
	public MsgAccept(WALEntry walEntry, long walIndex, byte[] entityGroup) {
		super(MSG_ID);
		this.walEntry = walEntry;
		this.walIndex = walIndex;
		this.entityGroup = entityGroup;
	}
	
	public AvroAccept toAvro() {
		AvroAccept avroAccept = new AvroAccept();
		if(walEntry != null) {
			avroAccept.walEntry = walEntry.toAvro();
		}
		avroAccept.walIndex = walIndex;
		avroAccept.entityGroup = ByteBuffer.wrap(entityGroup);
		return avroAccept;
	}
}
