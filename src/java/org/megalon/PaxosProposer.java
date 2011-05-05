package org.megalon;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PaxosProposer {
	// TODO make these configurable
	static public int DEFAULT_PREPARE_WAIT_MS = 1000;
	static public int DEFAULT_ACCEPT_WAIT_MS = 1000;
	
	Megalon megalon;
	PaxosServer paxosServer;
	Log logger = LogFactory.getLog(PaxosProposer.class);
	List<SingleWrite> queuedWrites = new LinkedList<SingleWrite>();
	
	public PaxosProposer(Megalon megalon) throws IOException {
		this.megalon = megalon;
		this.paxosServer = megalon.paxosServer;
	}
	
	/**
	 * Do a write as part of this transaction. This does no I/O, but just 
	 * stores the value in memory until commit time. Important note: you will
	 * not observe your own writes in the database until after you commit!
	 */
	public void write(byte[] table, byte[] cf, byte[] col, byte[] val) {
		queuedWrites.add(new SingleWrite(table, cf, col, val));
	}

	/**
	 * Commit this transaction synchronously (wait for success or failure).
	 * @return whether the transaction committed (if false, the database is 
	 * unchanged).
	 * @param eg The entity group to commit to. Every item written by this
	 * transaction must belong to this entity group!
	 * @param timeoutMs The transaction will fail automatically after this many
	 * milliseconds.
	 */
	public boolean commitSync(String eg, long timeoutMs) {
		WALEntry walEntry = new WALEntry(queuedWrites);
		Future<Boolean> f = paxosServer.commit(walEntry, eg, timeoutMs);
		try {
			while(true) {
				try { return f.get(); } catch (InterruptedException ie) {}
			} 
		} catch (ExecutionException ee) {
			logger.warn(ee);
			return false;
		}
	}
	
	/**
	 * Commit this transaction asynchronously. This function will return
	 * immediately. The returned future can be used to tell whether the
	 * transaction was successful, once it completes or times out.
	 * @param eg The entity group to commit to. Every item written by this
	 * transaction must belong to this entity group!
	 * @param timeoutMs The transaction will fail automatically after this many
	 * milliseconds.
	 * @return a Future that will eventually tell whether the transaction
	 * succeeded.
	 */
	public Future<Boolean> commitAsync(String eg, long timeoutMs) {
		WALEntry walEntry = new WALEntry(queuedWrites);
		return paxosServer.commit(walEntry, eg, timeoutMs);
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
}
