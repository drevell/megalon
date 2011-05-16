package org.megalon;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.Stage;

/**
 * This class will hold the responses from remote accepters while a 
 * non-blocking RPC call is in flight.
 */
public class ReplResponses<T extends AckPayload> {
	Log logger = LogFactory.getLog(AckPayload.class);
	
	Stage<T> nextStage = null;
	MultiStageServer<T> server = null;
	int expectedResponses = 0;
	Map<String, List<ByteBuffer>> remoteResponses = 
		new HashMap<String, List<ByteBuffer>>();
	boolean inited = false;
//	boolean ackLocal;
	boolean allResponses = false;
	boolean expectLocalResponse;
	boolean recvdLocalResponse;
	T payload;
	Object localResponse;
	int numAcks = 0;
	int numFails = 0;
	
	protected ReplResponses(MultiStageServer<T> server, Stage<T> nextStage, 
			int expectedResponses, T payload, boolean expectLocalResponse) {
		this.server = server;
		this.nextStage = nextStage;
		this.expectedResponses = expectedResponses;
		this.remoteResponses.clear();
		this.inited = true;
//		this.ackLocal = false;
		this.payload = payload;
		this.expectLocalResponse = expectLocalResponse;
		this.recvdLocalResponse = false;
	}
	
	/**
	 * Called by the RPCClient when a valid response is received.
	 */
	synchronized public void remoteResponse(String replica, List<ByteBuffer> response) {
		assert inited;
		logger.debug("Ack from " + replica + " with: " + 
				RPCUtil.strBufs(response));
		remoteResponses.put(replica, response);
		numAcks++;
		enqueueIfAllResponses();
	}
	
	/**
	 * Called by the RPCClient when a request times out.
	 */
	synchronized public void remoteFail(String replica) {
		assert inited;
		logger.debug("Fail from " + replica);
		remoteResponses.put(replica, null);
		numFails++;
		enqueueIfAllResponses();
	}
	
	synchronized public void localResponse(Object resp) {
		logger.debug("local ack");
		this.localResponse = resp;
		recvdLocalResponse = true;
		numAcks++;
		enqueueIfAllResponses();
	}
	
	synchronized public void localFail() {
		logger.debug("local fail");
		localResponse = null;
		recvdLocalResponse = true;
		numFails++;
		enqueueIfAllResponses();
	}
	
	/**
	 * Every ack or fail call will call this to check if a sufficient
	 * number of responses have been received.
	 */
	synchronized protected void enqueueIfAllResponses() {
		int numResponses = numAcks + numFails;
		assert numResponses <= expectedResponses;
		if(numResponses == expectedResponses) {
			server.enqueue(payload, nextStage, 
					payload.finisher);
			logger.debug("****** enqueueIfAllResponses: all responded, enqueueing");
			allResponses = true;
		} else {
			logger.debug("****** enqueueIfAllResponses: not enough yet");
		}
	}
	
	public Stage<T> getNextStage() {
		return nextStage;
	}
	
	public MultiStageServer<T> getServer() {
		return server;
	}
	
	/**
	 * Have we gotten as many acks & fails as we were waiting for?
	 */
	synchronized public boolean gotAllResponses() {
		return allResponses;
	}
	
	/**
	 * Returns the Object representing the local replica's reponse. Null if the
	 * response timed out or failed.
	 */
	synchronized public Object getLocalResponse() {
		return localResponse;
	}
	
	synchronized public Map<String,List<ByteBuffer>> getRemoteResponses() {
		return remoteResponses;
	}
	
	synchronized public int numResponses() {
		return numAcks + numFails;
	}
	
	public boolean isLocalResponse() {
		return expectLocalResponse;
	}
}