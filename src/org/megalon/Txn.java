package org.megalon;

public class Txn {
	public Txn() {
		return;
	}
	
	public byte[] read(String table, String cf, String col) {
		return null;
	}
	
	public void write(String table, String cf, String col, byte[] value) {
		return;
	}
	
	public boolean commit(Txn txn) {
		return false;
	}
}
