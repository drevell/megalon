package org.megalon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is the payload class for the server that handles the Paxos distributed
 * agreement among the replicas. It's passed between stages of the Paxos server. 
 */
public class MPaxPayload extends AckPayload {
	Log logger = LogFactory.getLog(MPaxPayload.class);	

	WALEntry requestedEntry; // The original value proposed by the client
	// We may end up preparing/accepting a value other than the one proposed by 
	// the client, if another client is also vying for this log position
	boolean proposedOwnValue = false; 
	WALEntry workingEntry; 
	long walIndex;
	
//	boolean isFinished = false;
	boolean committed = false;
	long finishTimeMs;
	byte[] eg;
	int commitTries = 0;
	
	public MPaxPayload(byte[] eg, WALEntry walEntry, long walIndex, long timeout) {
		super(timeout);
		this.eg = eg;
		this.requestedEntry = walEntry;
		this.workingEntry = walEntry;
		this.walIndex = walIndex;
	}
	
	public boolean isCommitted() {
		return committed;
	}
}
