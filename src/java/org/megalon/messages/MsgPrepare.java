package org.megalon.messages;

import org.megalon.avro.AvroPrepare;

public class MsgPrepare implements MegalonMsg {
	public final long n;
	public final long walIndex;
	public final byte[] entityGroup;
	
	
	public MsgPrepare(AvroPrepare avroPrepare) {
		this.n = avroPrepare.n;
		this.walIndex = avroPrepare.walIndex;
		
		this.entityGroup = new byte[avroPrepare.entityGroup.remaining()];
		avroPrepare.entityGroup.get(this.entityGroup);
	}
}
