package org.megalon;

import java.io.IOException;
import java.util.Collection;

import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config.Host;
import org.megalon.Config.ReplicaDesc;
import org.megalon.PaxosSocketMultiplexer.RespWaiter;
import org.megalon.avro.AvroAcceptResponse;
import org.megalon.avro.AvroPaxosProto;
import org.megalon.avro.AvroPrepareResponse;

@SuppressWarnings("deprecation")
public class PaxosProposer {
	Megalon megalon;
	WAL wal;
	Log logger = LogFactory.getLog(PaxosProposer.class);
	
	public PaxosProposer(Megalon megalon) throws IOException {
		this.megalon = megalon;
		wal = new WAL(megalon);
	}
	
	/**
	 * Paxos accept(N, value), which is called after a prepare(N) message has
	 * been acknowledged by a quorum of accepters.
	 */
	public void accept(long walIndex, WALEntry walEntry, int waitMs) throws 
	NoQuorum {
		Collection<ReplicaDesc> replicas = megalon.config.replicas.values();
		int numReplicas = replicas.size();
		RespWaiter<AvroAcceptResponse> waiter = 
			new RespWaiter<AvroAcceptResponse>(numReplicas);
		for(ReplicaDesc replicaDesc: replicas) {
			Host host = replicaDesc.replsrv.get(0); // TODO host selection
			// TODO create multiplexers on start and cache them
			try {
				PaxosSocketMultiplexer mplex = new PaxosSocketMultiplexer(host);
				mplex.accept(walIndex, walEntry.toAvro(), waiter);
			} catch (IOException e) {
				waiter.nack(host);
			}
		}
		waiter.waitForQuorum(waitMs);
	}
	
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
	public WALEntry prepare(long walIndex, long n, int waitMs) throws NoQuorum {
		Collection<ReplicaDesc> replicas = megalon.config.replicas.values();
		int numReplicas = replicas.size();
		RespWaiter<AvroPrepareResponse> waiter = 
			new RespWaiter<AvroPrepareResponse>(numReplicas);
		for(ReplicaDesc replicaDesc: replicas) {
			Host host = replicaDesc.replsrv.get(0); // TODO host selection
			// TODO create multiplexers on start and cache them
			try {
				PaxosSocketMultiplexer mplex = new PaxosSocketMultiplexer(host);
				mplex.prepare(walIndex, n, waiter);
			} catch (IOException e) {
				waiter.nack(host);
			}
		}
		waiter.waitForQuorum(waitMs);
		long bestN = -1;
		WALEntry bestEntry = null;
		for(AvroPrepareResponse resp: waiter.getResponses()) {
			if(resp.walEntry.n > bestN) {
				bestN = resp.walEntry.n;
				bestEntry = new WALEntry(resp.walEntry);
			}
		}
		return bestEntry;
	}

	
	/**
	 * Attempt a Paxos accept in a remote replica via Avro RPC.
	 * @return boolean whether the replica ack'ed the accept and logged it.
	 */
	public boolean acceptRemote(ReplicaDesc replDesc, int walIndex, 
			WALEntry entry) throws IOException {
		Host host = replDesc.replsrv.get(0); // TODO host selection
		// TODO share multiplexers among proposers
		PaxosSocketMultiplexer mplex = new PaxosSocketMultiplexer(host);
//		RespWaiter<AvroAcceptResponse> waiter = new RespWaiter();
		throw new AssertionError("TODO unfinished");
	}
	

	/**
	 * Run a Paxos prepare on the given replica.
	 * @return null: Paxos ack. Non-null WALEntry: a previously-seen proposal. 
	 */
	public WALEntry prepareRemote(ReplicaDesc replDesc, int walIndex, int n) 
	throws IOException {
		throw new AssertionError("TODO unfinished");
	}
}
