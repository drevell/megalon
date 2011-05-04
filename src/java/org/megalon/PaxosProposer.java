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
//	WAL wal;
	Log logger = LogFactory.getLog(PaxosProposer.class);
	List<SingleWrite> queuedWrites = new LinkedList<SingleWrite>();
	
	public PaxosProposer(Megalon megalon) throws IOException {
		this.megalon = megalon;
//		this.wal = new WAL(megalon);
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
	
	
//	/**
//	 * Delete this function after read() works. Writers should not know
//	 * walIndex or N. TODO real commit()
//	 */
//	public boolean hack_commit(int waitMs, long _hackWalIndex, long _hackN) {
//		WALEntry entry;
//		try {
//			
////			entry = prepare(_hackWalIndex, _hackN, waitMs);
//		} catch (NoQuorum e) {
//			logger.warn("hack_commit: prepare failed quorum");
//			return false;
//		}
//		if(entry != null) {
//			logger.debug("hack_commit accepting preexisting value");
//		} else {
//			logger.debug("No preexisting value, proposing new value");
//			entry = new WALEntry(_hackN, queuedWrites, Status.ACCEPTED);
//		}
//		return accept(_hackWalIndex, entry, waitMs);
//	}
	
	/**
	 * Paxos prepare(N), which is the first phase of adding a value to the
	 * WAL. This function should be called only by a Paxos proposer (who is 
	 * following the Paxos rules).
	 * 
	 * @param n The Paxos proposal number (unique and increasing)
	 * @return A PaxEntryState object describing the values that the accepters
	 * have already seen. If the returned object has a non-null value,  then the 
	 * proposer is obligated to accept()the returned (value,n) instead of its 
	 * own value. If the returned object has a null value, then the proposer can 
	 * issue accept() for its desired value.
	 */
//	public WALEntry prepare(long walIndex, long n, int waitMs) 
//	throws NoQuorum {
//		// TODO get n from local replica, don't take it as input
//		Collection<ReplicaDesc> replicas = megalon.config.replicas.values();
//		ClientSharedData clientData = megalon.clientData;
//		int numReplicas = replicas.size();
////		RespWaiter<WALEntry> waiter = new RespWaiter<WALEntry>(numReplicas);
//		// Send a prepare message to each replica
//		for(ReplicaDesc replicaDesc: replicas) {
//			if(replicaDesc == megalon.config.myReplica) {
//				// The local replica is a special case
//				try {
//					WALEntry response = wal.prepareLocal(walIndex, n);
//					waiter.response(replicaDesc.name, response);
//				} catch (IOException e) {
//					logger.warn("prepare local replica IOException", e);
//					waiter.nack(replicaDesc.name);
//				}
//				continue;
//			}
//			byte[] encodedMsg = avroEncodePrepare()
//			boolean sentToThisReplica = false;
//			ByteBuffer[] outBytes = avroEncodedPrepare(walIndex, n, payload);
//			for(Host host: (List<Host>)Util.shuffled(replicaDesc.replsrv)) {
//				try {
//					PaxosSocketMultiplexer sock = 
//						clientData.getReplSrvSocket(host);
//					sock.prepare(walIndex, n, waiter);
//					sentToThisReplica = true;
//				} catch (IOException e) {
//					logger.debug("Host prepare failed: " + host, e);
//				}
//			}
//			if(!sentToThisReplica) {
//				logger.debug("Couldn't send prepare to replica: " + 
//						replicaDesc.name);
//				waiter.nack(replicaDesc.name);
//			}
//		}
//		if(!waiter.waitForQuorum(waitMs)) {
//			throw new NoQuorum();
//		}
//
//		// Pick the accepted value with the highest N from among the responses
//		WALEntry bestEntry = null;
//		for(WALEntry entry: waiter.getResponses()) {
//			if(bestEntry == null || entry.n > bestEntry.n) {
//				bestEntry = entry;
//			}
//		}
//		return bestEntry;
//	}
//	
//	/**
//	 * Wrapper around prepare() that uses the default timeout.
//	 */
//	public WALEntry prepare(long walIndex, long n) throws NoQuorum {
//		return prepare(walIndex, n, DEFAULT_PREPARE_WAIT_MS);
//	}
	
//	protected List<ByteBuffer> avroEncodedPrepare(long walIndex, long n) {
//		AvroPrepare msg = new AvroPrepare();
//		msg.n = n;
//		msg.walIndex = walIndex;
//
//		ByteBufferOutputStream os = new ByteBufferOutputStream();
//		
//	}
	
//	/**
//	 * Paxos accept(N, value), which is called after a prepare(N) message has
//	 * been acknowledged by a quorum of accepters.
//	 */
//	public boolean accept(long walIndex, WALEntry walEntry, int waitMs) {
//		Collection<ReplicaDesc> replicas = megalon.config.replicas.values();
//		int numReplicas = replicas.size();
//		ClientSharedData clientData = megalon.clientData;
//		RespWaiter<Object> waiter = 
//			new RespWaiter<Object>(numReplicas);
//		// Send an accept message to each replica
//		for(ReplicaDesc replicaDesc: replicas) {
//			if(replicaDesc == megalon.config.myReplica) {
//				// The local replica is a special case
//				boolean response = false;
//				try {
//					response = wal.acceptLocal(walIndex, walEntry);
//				} catch (IOException e) {
//					logger.warn("prepare local replica IOException", e);
//					response = false;
//				}
//				if(response) {
//					waiter.response(replicaDesc.name, response);
//				} else {
//					waiter.nack(replicaDesc.name);
//				}
//				continue;
//			}
//			boolean sentToThisReplica = false;
//			for(Host host: (List<Host>)Util.shuffled(replicaDesc.replsrv)) {
//				try {
//					PaxosSocketMultiplexer sock = 
//						clientData.getReplSrvSocket(host);
//					sock.accept(walIndex, walEntry.toAvro(), waiter);
//					sentToThisReplica = true;
//					break;
//				} catch (IOException e) {
//					logger.debug("Accept IOException, trying next host", e);
//				}
//			}
//			if(!sentToThisReplica) {
//				logger.debug("Couldn't send accept to replica: " + 
//						replicaDesc.name);
//				waiter.nack(replicaDesc.name);
//			}
//		}
//		return waiter.waitForQuorum(waitMs);
//	}
//	
//	/**
//	 * A wrapper around accept() that uses the default timeout.
//	 */
//	public boolean accept(long walIndex, WALEntry entry) {
//		return accept(walIndex, entry, DEFAULT_ACCEPT_WAIT_MS);
//	}
}
