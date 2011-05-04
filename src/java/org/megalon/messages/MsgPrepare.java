package org.megalon.messages;

import org.megalon.avro.AvroPrepare;

public class MsgPrepare implements MegalonMsg {
	public long n;
//	public long reqSerial;
	public long walIndex;
	
	public MsgPrepare(AvroPrepare avroPrepare) {
		this.n = avroPrepare.n;
//		this.reqSerial = avroPrepare.reqSerial;
		this.walIndex = avroPrepare.walIndex;
	}
}
