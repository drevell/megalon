package org.megalon;

/**
 * This represents the response to a Paxos prepare or accept message.
 */
public class PaxResp {
	static public enum Type {ACK, NACK};
	
	Type type;
	WALEntry walEntry;
	
	public PaxResp(Type type, WALEntry w) {
		this.type = Type.ACK;
		this.walEntry = w;
	}
	
}
