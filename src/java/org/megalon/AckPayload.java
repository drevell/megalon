package org.megalon;

import org.megalon.multistageserver.Payload;

/**
 * When making an RPC call using an RPCClient, you provide an AckPayload object
 * to the RPCClient in order to be notified when the RPC call completes (or 
 * fails).
 */
public class AckPayload extends Payload {
	ReplResponses<? extends AckPayload> replResponses;
	long finishTimeMs;
	
	public AckPayload(long timeout) {
		finishTimeMs = System.currentTimeMillis() + timeout;
	}
	
	public long getFinishTimeMs() {
		return finishTimeMs;
	}
}
	

