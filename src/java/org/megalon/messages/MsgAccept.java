package org.megalon.messages;

import org.megalon.WALEntry;
import org.megalon.avro.AvroAccept;

public class MsgAccept implements MegalonMsg {
	public WALEntry walEntry;
//	public long reqSerial;
	public long walIndex;
	
	public MsgAccept(AvroAccept avroAccept) {
		this.walEntry = new WALEntry(avroAccept.walEntry);
//		this.reqSerial = avroAccept.reqSerial;
		this.walIndex = avroAccept.walIndex;
	}
}
