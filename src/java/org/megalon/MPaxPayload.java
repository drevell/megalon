package org.megalon;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.Stage;
import org.megalon.multistageserver.Payload;

/**
 * This is the payload class for the server that handles the Paxos distributed
 * agreement among the replicas. It's passed between stages of the Paxos server. 
 */
public class MPaxPayload extends Payload {
	WALEntry requestedEntry; // The original value proposed by the client

	// We may end up preparing/accepting a value other than the one proposed by 
	// the client, if another client is also vying for this log position
	boolean usedExisting = false; 
	WALEntry workingEntry; 
	long walIndex;
	ReplResponses replResponses = new ReplResponses();
	boolean isFinished = false;
	boolean committed = false;
	long finishTimeMs;
	String eg;
	
	public MPaxPayload(String eg, WALEntry walEntry, long walIndex, long timeout) {
		this.eg = eg;
		this.requestedEntry = walEntry;
		this.workingEntry = walEntry;
		this.walIndex = walIndex;
		finishTimeMs = System.currentTimeMillis() + timeout;
	}
	
	/**
	 * This class will hold the responses from remote accepters while a 
	 * non-blocking propose or accept is in flight. When all responses have been
	 * received (including timeouts), ack() and nack() will return the next
	 * stage. The caller should enqueue this payload in that stage to resume
	 * processing of the request.
	 */
	public class ReplResponses {
		Stage<MPaxPayload> nextStage = null;
		MultiStageServer<MPaxPayload> server = null;
		int expectedResponses = 0;
		Map<String, List<ByteBuffer>> responses = 
			new HashMap<String, List<ByteBuffer>>();
		boolean inited = false;
		
		protected ReplResponses() {}
		
		public void init(MultiStageServer<MPaxPayload> server, 
				Stage<MPaxPayload> nextStage, int expectedResponses) {
			this.server = server;
			this.nextStage = nextStage;
			this.expectedResponses = expectedResponses;
			this.responses.clear();
			this.inited = true;
		}
		
		/**
		 * Called by the RPCClient when a valid response is received.
		 */
		synchronized public void ack(String replica, List<ByteBuffer> response) {
			assert inited;
			responses.put(replica, response);
			enqueueIfAllResponses();
		}
		
		/**
		 * Called be the RPCClient when a request times out.
		 */
		synchronized public void nack(String replica) {
			assert inited;
			responses.put(replica, null);
			enqueueIfAllResponses();
		}
		
		/**
		 * Every ack or nack call will call this to check if a sufficient
		 * number of responses have been received.
		 */
		synchronized protected void enqueueIfAllResponses() {
			if(responses.size() == expectedResponses) {
				server.enqueue(MPaxPayload.this, nextStage, 
						MPaxPayload.this.finisher);
			}
		}
		
		public Stage<MPaxPayload> getNextStage() {
			return nextStage;
		}
		
		public MultiStageServer<MPaxPayload> getServer() {
			return server;
		}
	}
}
