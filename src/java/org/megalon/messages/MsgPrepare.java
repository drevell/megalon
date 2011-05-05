package org.megalon.messages;

import org.megalon.avro.AvroPrepare;

public class MsgPrepare implements MegalonMsg {
	public long n;
	public long walIndex;
	
	public MsgPrepare(AvroPrepare avroPrepare) {
		this.n = avroPrepare.n;
		this.walIndex = avroPrepare.walIndex;
	}
}
