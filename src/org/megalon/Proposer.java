package org.megalon;

public class Proposer {
	public Proposer(Config conf) {
		
	}
	
	static public byte[] snapshotRead(String table, String cf, String col) {
		return null;
	}
	
	static public byte[] inconsRead(String table, String cf, String col) {
		return null;
	}

	static public boolean unsafeWrite(String table, String cf, String col, 
			byte[] val) {
		return false;
	}
	
	
	public static class ProposerStatus {
		
	}
}
