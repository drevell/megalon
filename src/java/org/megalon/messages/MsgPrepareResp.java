package org.megalon.messages;

import org.megalon.WALEntry;
import org.megalon.avro.AvroPrepareResponse;

public class MsgPrepareResp implements MegalonMsg {
	public WALEntry walEntry;
//	public long reqSerial;
	public boolean hadQuorum;
	
	public MsgPrepareResp(WALEntry walEntry, boolean hadQuorum) {
		this.walEntry = walEntry;
//		this.reqSerial = reqSerial;
		this.hadQuorum = hadQuorum;
	}
	
	public AvroPrepareResponse toAvro() {
		AvroPrepareResponse avroResp = new AvroPrepareResponse();
//		avroResp.reqSerial = reqSerial;
		avroResp.hadQuorum = hadQuorum;
		if(avroResp.walEntry != null) {
			avroResp.walEntry = walEntry.toAvro();
		}
		return avroResp;
	}
}
