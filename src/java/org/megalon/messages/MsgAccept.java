package org.megalon.messages;

import org.megalon.WALEntry;
import org.megalon.avro.AvroAccept;

public class MsgAccept implements MegalonMsg {
	public WALEntry walEntry;
	public long walIndex;
	
	public MsgAccept(AvroAccept avroAccept) {
		this.walEntry = new WALEntry(avroAccept.walEntry);
		this.walIndex = avroAccept.walIndex;
	}
	
	public MsgAccept(WALEntry walEntry, long walIndex) {
		this.walEntry = walEntry;
		this.walIndex = walIndex;
	}
	
	public AvroAccept toAvro() {
		AvroAccept avroAccept = new AvroAccept();
		if(walEntry != null) {
			avroAccept.walEntry = walEntry.toAvro();
		}
		avroAccept.walIndex = walIndex;
		return avroAccept;
	}
}
