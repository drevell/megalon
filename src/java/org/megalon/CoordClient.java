package org.megalon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config.Host;
import org.megalon.Config.ReplicaDesc;
import org.megalon.messages.MsgCheckValid;
import org.megalon.messages.MsgCheckValidResp;
import org.megalon.messages.MsgValidate;
import org.megalon.messages.MsgValidateResp;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.Finisher;
import org.megalon.multistageserver.MultiStageServer.NextAction;
import org.megalon.multistageserver.MultiStageServer.NextAction.Action;
import org.megalon.multistageserver.MultiStageServer.Stage;

/**
 * This is a MultiStageServer that acts as a non-blocking client for coordinator
 * requests. Clients can run a "CheckValid" operation or a "Validate" operation in either
 * blocking or non-blocking modes.
 * 
 * "CheckValid" asks the coordinator whether it thinks the local replica is up 
 * to date. Readers will execute a CheckValid when they're deciding which 
 * replica to read from. 
 *  
 * "Validate" informs the coordinator whether the local replica is, in 
 * fact, up to date. This will be run by writers when they are committing.
 * 
 * See the MegaStore paper for a high-level description of these operations.
 */
public class CoordClient {
	Log logger = LogFactory.getLog(CoordClient.class);
	Megalon megalon;
	
	// This server will handle non-blocking RPC requests to remote replicas
	MultiStageServer<CoordCliPayload> coordCliServer;
	ChkvEncStage chkvEncStage;
	ChkvrespDecStage chkvrespDecStage;
	ValidateEncStage validateEncStage;
	ValidateRespDecStage validateRespDecStage;
	
	public CoordClient(Megalon megalon) throws IOException {
		this.megalon = megalon;
		chkvEncStage = new ChkvEncStage();
		chkvrespDecStage = new ChkvrespDecStage();
		validateEncStage = new ValidateEncStage();
		validateRespDecStage = new ValidateRespDecStage(); 
		Set<Stage<CoordCliPayload>> stages = new HashSet<Stage<CoordCliPayload>>();
		stages.add(chkvEncStage);
		stages.add(chkvrespDecStage);
		stages.add(validateEncStage);
		stages.add(validateRespDecStage);
		coordCliServer = new MultiStageServer<CoordCliPayload>("coordCli", stages);
	}

	/**
	 * Begin a non-blocking CheckValid operation, returning a Future that will
	 * eventually give the result. The Future returns boolean whether the given 
	 * replica is up to date.
	 */
	public Future<Boolean> checkValidAsync(String replica, byte[] entityGroup, 
	long timeoutMs) throws IOException {
		if(replica.equals(megalon.config.myReplica.name) && megalon.config.run_coord) {
			// The query is for this replica, and we have a coordinator process
			// in this JVM. Use it instead of using RPC.
			return megalon.getCoord().checkValidLocal(entityGroup, timeoutMs);
		}
		if(!megalon.config.replicas.containsKey(replica)) {
			throw new IOException("No such replica: " + replica);
		}
		CoordCliPayload payload = new CoordCliPayload(entityGroup, replica, timeoutMs);
		coordCliServer.enqueue(payload, chkvEncStage);
		return new IsValidFuture(payload);
	}
	
	/**
	 * Do a blocking CheckValid operation. Returns boolean whether the given
	 * replica is up to date.
	 */
	public boolean checkValidSync(String replica, byte[] entityGroup, 
	long timeoutMs) throws IOException {
		try {
			return checkValidAsync(replica, entityGroup, timeoutMs).get();
		} catch (ExecutionException e) {
			logger.info("Exception in CoordClient checkValidSync", e);
			throw new IOException("checkValid failed", e.getCause());
		} catch (InterruptedException e) {
			logger.info("checkValidSync interrupted", e);
			throw new IOException("checkValidSync interrupted", e);
		}
	}
	
	/**
	 * Begin a non-blocking Validate operation for each of the given replicas.
	 * @param validate True to validate, false to invalidate
	 */
	public Future<Map<String,Boolean>> validateAsync(List<ReplicaDesc> replicas, 
			byte[] entityGroup, long walIndex, long timeoutMs, boolean validate) {
		CoordCliPayload payload = new CoordCliPayload(entityGroup, replicas, 
				timeoutMs, walIndex, validate);
		coordCliServer.enqueue(payload, validateEncStage);
		return new ValidateFuture(payload);
	}
	
	/**
	 * Do a blocking Validate operation on each of the given replicas.
	 * @param validate True to validate, false to invalidate.
	 */
	public Map<String,Boolean> validateSync(List<ReplicaDesc> replicas, 
	byte[] entityGroup, long walIndex, long timeoutMs, boolean validate) 
	throws IOException {
		Future<Map<String,Boolean>> future = validateAsync(replicas, 
				entityGroup, walIndex, timeoutMs, validate);
		
		Exception storedException;
		try {
			return future.get();
		} catch (ExecutionException e) {
			storedException = e;
		} catch (InterruptedException e) {
			storedException = e;
		}
		logger.info("validate exception", storedException);
		throw new IOException(storedException);
	}
	
	/**
	 * This server stage encodes an outgoing CheckValid request as Avro and
	 * sends it via the appropriate RPCClient to reach the desired replica.
	 */
	class ChkvEncStage implements Stage<CoordCliPayload> {
		/**
		 * This finisher callback will be called when a local coordinator 
		 * finishes handling a request. It just calls ackLocal on the 
		 * ReplResponses object that's waiting for all replica responses.
		 */
		class LocalCoordFinisher implements Finisher<MPayload> {
			CoordCliPayload cliPayload;
			
			public LocalCoordFinisher(CoordCliPayload cliPayload) {
				this.cliPayload = cliPayload;
			}
			
			public void finish(MPayload svrPayload) {
				assert svrPayload.resp instanceof MsgCheckValidResp;
				MsgCheckValidResp r = (MsgCheckValidResp)svrPayload.resp;
				if(r == null) {
					cliPayload.replResponses.localFail();
				} else {
					cliPayload.replResponses.localResponse(r.isValid);
				}
			}
		}
		
		public NextAction<CoordCliPayload> runStage(CoordCliPayload payload)
				throws Exception {
			assert payload.walIndex == -1; // payload.walIndex unused for checkValid call
			assert payload.checkValidReplica != null; // checkvalid hits only 1 replica
			
			if(payload.checkValidReplica.equals(megalon.config.myReplica) && 
					megalon.config.run_coord) {
				// The target coordinator is in this JVM. Skip Avro encoding and
				// enqueue directly into the coordinator.
				MPayload coordPayload = new MPayload(MsgCheckValid.MSG_ID, 
						new MsgCheckValid(payload.entityGroup), null);
				
				// We're waiting for one response, at which point processing should
				// resume in rpcRespStage.
				payload.replResponses = new ReplResponses<CoordCliPayload>(
						coordCliServer, chkvrespDecStage, 1, payload, true);
				
				Coordinator c = megalon.getCoord();
				c.coreServer.enqueue(coordPayload, c.execStage, 
						new LocalCoordFinisher(payload));
			} else {
				// The target coordinator is not in this JVM. Use Avro.
				List<ByteBuffer> outBufs = RPCUtil.rpcBbEncode(
						new MsgCheckValid(payload.entityGroup));

				// We're waiting for one response, at which point processing should
				// resume in rpcRespStage.
				payload.replResponses = new ReplResponses<CoordCliPayload>(
						coordCliServer, chkvrespDecStage, 1, payload, false);
				
				logger.debug("Getting RPCClient for replica: " + 
						payload.checkValidReplica);
				RPCClient rpcClient = megalon.clientData.coordSockets.get(
						payload.checkValidReplica);
				assert rpcClient != null;
				rpcClient.write(outBufs, payload);
			}
			
			return new NextAction<CoordCliPayload>(Action.IGNORE, null);
		}

		public int getNumConcurrent() {
			return 10;
		}

		public String getName() {
			return this.getClass().getCanonicalName();
		}

		public int getBacklogSize() {
			return 10;
		}

		public void setServer(MultiStageServer<CoordCliPayload> server) {}		
	}

	/**
	 * When the remote server responds to a CheckValid request, this stage 
	 * decodes its Avro message into single boolean response, which is stored in
	 * the payload object. At this point the server is finished with the
	 * CheckValid operation.
	 */
	class ChkvrespDecStage implements Stage<CoordCliPayload> {
		public NextAction<CoordCliPayload> runStage(CoordCliPayload payload)
				throws Exception {
			assert payload.replResponses.gotAllResponses();
			assert payload.replResponses.numResponses() == 1;

			if(payload.replResponses.expectLocalResponse) {
				// We used a local coordinator (not Avro RPC)
				payload.egValid = (Boolean)payload.replResponses.getLocalResponse();
			} else {
				Map<String, List<ByteBuffer>> responses =
					payload.replResponses.getRemoteResponses();
				List<ByteBuffer> bbList = responses.values().iterator().next();
				if(bbList == null) {
					payload.egValid = false;
				} else {
					byte msgId = RPCUtil.extractByte(bbList);
					if(msgId != MsgCheckValidResp.MSG_ID) {
						logger.debug("Expected MsgCheckValidResp, got msgId " + 
								msgId);
					}
					MsgCheckValidResp msg = (MsgCheckValidResp)RPCUtil.rpcDecode(
							msgId, bbList);
					payload.egValid = msg.isValid;
				}
			}	
			return new NextAction<CoordCliPayload>(Action.FINISHED, null);
		}

		public int getNumConcurrent() {
			return 10;
		}

		public String getName() {
			return this.getClass().getCanonicalName();
		}

		public int getBacklogSize() {
			return 10;
		}

		public void setServer(MultiStageServer<CoordCliPayload> server) {}		
	}
	
	/**
	 * This stage sends a Validate message to a list of replicas. It encodes an
	 * Avro message and sends to each replica.
	 */
	public class ValidateEncStage implements Stage<CoordCliPayload> {
		class LocalFinisher implements Finisher<MPayload> {
			CoordCliPayload cliPayload;
			
			public LocalFinisher(CoordCliPayload cliPayload) {
				this.cliPayload = cliPayload;
			}
			
			public void finish(MPayload payload) {
				if(payload.resp != null) {
					MsgValidateResp resp = (MsgValidateResp)payload.resp;
					cliPayload.replResponses.localResponse(resp.acked);
				} else {
					cliPayload.replResponses.localFail();
				}
			}
		}
		
		public NextAction<CoordCliPayload> runStage(CoordCliPayload payload) 
		throws IOException {
			assert payload.walIndex >= 0;
			
			// Encode the outgoing message as bytes using avro
			List<ByteBuffer> outBufs = RPCUtil.rpcBbEncode(new MsgValidate(
					payload.entityGroup, payload.walIndex, payload.validate));
			
			boolean coordInJvm = false;
			if(megalon.config.run_coord) {
				for(ReplicaDesc replDesc: payload.replicas) {
					if(replDesc.name.equals(megalon.config.myReplica.name)) {
						coordInJvm = true;
					}
				}
			}
			
			// We will wait for all replicas to respond or fail, at which point
			// we'll continue processing in the validateRespDecStage
			payload.replResponses = new ReplResponses<CoordCliPayload>(
					coordCliServer, validateRespDecStage, 
					payload.replicas.size(), payload, coordInJvm);
			
			// Write the encoded validate message to all requested replicas
			for(ReplicaDesc replDesc: payload.replicas) {
				List<ByteBuffer> bufsForReplica = RPCUtil.duplicateBufferList(outBufs);
				String replicaName = replDesc.name;
				if(coordInJvm && replicaName.equals(megalon.config.myReplica.name)) {
					Coordinator c = megalon.getCoord();
					MsgValidate reqMsg = new MsgValidate(payload.entityGroup, 
							payload.walIndex, payload.isEgValid()); 
					MPayload svrPayload = new MPayload(MsgValidate.MSG_ID,
							reqMsg, null);
					Finisher<MPayload> finisher = new LocalFinisher(payload);
					c.coreServer.enqueue(svrPayload, c.execStage, finisher);
				} else {
					List<Host> coords = replDesc.coord;
					assert coords.size() == 1; // no clustered coordinators yet
//					Host coord = coords.get(0);
					RPCClient rpcClient = megalon.clientData.coordSockets.get(
							replDesc.name);
					rpcClient.write(bufsForReplica, payload);					
				}
			}
			
			return new NextAction<CoordCliPayload>(Action.IGNORE, null);
		}

		public int getNumConcurrent() {
			return 10;
		}

		public String getName() {
			return this.getClass().getCanonicalName();
		}

		public int getBacklogSize() {
			return 10;
		}

		public void setServer(MultiStageServer<CoordCliPayload> server) {}
		
	}
	/**
	 * This stage handles the responses to a Validate message. When all 
	 * responses have been received (or the timeout expired), this stage will
	 * run. It decodes the Avro responses, invalidates the replicas that timed
	 * out, and sets up a mapping String->Boolean indicating which replicas
	 * completed the operation.
	 */
	public class ValidateRespDecStage implements Stage<CoordCliPayload> {
		public NextAction<CoordCliPayload> runStage(CoordCliPayload payload) 
		throws IOException {
			assert payload.replResponses.gotAllResponses();
			assert payload.replResponses.numResponses() == payload.replicas.size();
			
			Map<String,List<ByteBuffer>> remoteResponses = 
				payload.replResponses.getRemoteResponses();
			for(Entry<String,List<ByteBuffer>> e: remoteResponses.entrySet()) {
				String replica = e.getKey();
				List<ByteBuffer> msgBufs = e.getValue();
				if(msgBufs == null) {
					// Coordinator timed out. TODO implement lock release wait stage
					String msg = "Coordinator failed: " + e.getKey() + 
						", would do lock release waiting now";
					logger.info(msg);
					throw new IOException(msg);
					//payload.validate(replica, false);

				} else {
					// We don't actually need to decode the response, there's
					// currently no reason a validate message could be nacked
					payload.validate(replica, true);
				}
			}
			
			if(payload.replResponses.expectLocalResponse) {
				// Assume any response is ack, no other response possible
				if(payload.replResponses.getLocalResponse() == null) {
					String msg = "Local coordinator failed, would wait for " +
						"lock release now";
					logger.info(msg);
					throw new IOException(msg);
					//payload.validate(megalon.config.myReplica.name, false);
				} else {
					payload.validate(megalon.config.myReplica.name, true);
				}
			}
			
			logger.debug("All replica coordinators acked");
			return new NextAction<CoordCliPayload>(Action.FINISHED, null);
		}

		public int getNumConcurrent() {
			return 10;
		}

		public String getName() {
			return this.getClass().getCanonicalName();
		}

		public int getBacklogSize() {
			return 10;
		}

		public void setServer(MultiStageServer<CoordCliPayload> server) {}
	}
	
	/**
	 * The eventual result of a non-blocking CheckValid operation.
	 */
	static public class IsValidFuture implements Future<Boolean> {
		CoordCliPayload payload;
		
		public IsValidFuture(CoordCliPayload payload) {
			this.payload = payload;
		}
		
		public boolean cancel(boolean arg0) {
			throw new UnsupportedOperationException();
		}

		public Boolean get() throws InterruptedException, ExecutionException {
			payload.waitFinished();
			assert payload.finished();
			return payload.isEgValid();
		}

		public Boolean get(long waitTime, TimeUnit units)
				throws InterruptedException, ExecutionException,
				TimeoutException {
			payload.waitFinished(waitTime, units);
			return payload.isEgValid();
		}

		public boolean isCancelled() {
			return false;
		}

		public boolean isDone() {
			return payload.finished();
		}
	}
	
	/**
	 * The eventual result of a non-blocking Validate operation.
	 */
	static public class ValidateFuture implements Future<Map<String,Boolean>> {
		CoordCliPayload payload;
		
		public ValidateFuture(CoordCliPayload payload) {
			this.payload = payload;
		}
		
		public boolean cancel(boolean arg0) {
			throw new UnsupportedOperationException();
		}

		public Map<String,Boolean> get() throws InterruptedException, 
		ExecutionException {
			payload.waitFinished();
			assert payload.finished();
			return payload.validated;
		}

		public Map<String,Boolean> get(long waitTime, TimeUnit units)
				throws InterruptedException, ExecutionException,
				TimeoutException {
			payload.waitFinished(waitTime, units);
			return payload.validated;
		}

		public boolean isCancelled() {
			return false;
		}

		public boolean isDone() {
			return payload.finished();
		}
	}
}
