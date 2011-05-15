package org.megalon.messages;

import org.megalon.WALEntry;
import org.megalon.avro.AvroPrepareResponse;

public class MsgPrepareResp extends MegalonMsg {
	public static final byte MSG_ID = 2;
	
	public WALEntry walEntry;
	public boolean hadQuorum;
	
	public MsgPrepareResp(WALEntry walEntry, boolean hadQuorum) {
		super(MSG_ID);
		this.walEntry = walEntry;
		this.hadQuorum = hadQuorum;
	}
	
	public AvroPrepareResponse toAvro() {
		AvroPrepareResponse avroResp = new AvroPrepareResponse();
		avroResp.hadQuorum = hadQuorum;
		if(avroResp.walEntry != null) {
			avroResp.walEntry = walEntry.toAvro();
		}
		return avroResp;
	}
	
	public String toString() {
		return "MsgPrepareResp walEntry=" + walEntry + " hadQuorum=" + hadQuorum;
	}
}
