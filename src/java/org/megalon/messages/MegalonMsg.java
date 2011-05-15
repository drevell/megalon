package org.megalon.messages;

import org.apache.avro.specific.SpecificRecordBase;

abstract public class MegalonMsg {
	byte msgId;
	
	private MegalonMsg() {}
	
	MegalonMsg(byte msgId) {
		this.msgId = msgId;
	}
	
	abstract public SpecificRecordBase toAvro();
	
	public byte getMsgId() {
		return msgId; 
	}
}
