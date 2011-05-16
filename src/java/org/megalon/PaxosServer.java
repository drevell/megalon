package org.megalon;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config.Host;
import org.megalon.Config.ReplicaDesc;
import org.megalon.WALEntry.Status;
import org.megalon.avro.AvroAccept;
import org.megalon.avro.AvroAcceptResponse;
import org.megalon.avro.AvroPrepare;
import org.megalon.avro.AvroPrepareResponse;
import org.megalon.messages.MsgAccept;
import org.megalon.messages.MsgAcceptResp;
import org.megalon.messages.MsgPrepare;
import org.megalon.messages.MsgPrepareResp;
import org.megalon.multistageserver.BBInputStream;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.Finisher;
import org.megalon.multistageserver.MultiStageServer.NextAction;
import org.megalon.multistageserver.MultiStageServer.NextAction.Action;
import org.megalon.multistageserver.MultiStageServer.Stage;

public class PaxosServer {
	Log logger = LogFactory.getLog(PaxosServer.class);
	MultiStageServer<MPaxPayload> server;
	Megalon megalon;
	PaxosPrepareStage sendPrepareStage;
	PaxosAcceptStage sendAcceptStage;
	PaxosRespondStage respondStage;
	WAL wal;
	
	public PaxosServer(Megalon megalon) throws Exception {
		this.megalon = megalon;
		this.wal = new WAL(megalon);
		Set<Stage<MPaxPayload>> serverStages = new HashSet<Stage<MPaxPayload>>();
		respondStage = new PaxosRespondStage();
		sendAcceptStage = new PaxosAcceptStage();
		sendPrepareStage = new PaxosPrepareStage();
		
		serverStages.add(sendPrepareStage);
		serverStages.add(sendAcceptStage);
		serverStages.add(respondStage);
		
		this.server = new MultiStageServer<MPaxPayload>("paxosSvr", serverStages);
	}
	
	public Future<Boolean> commit(WALEntry walEntry, byte[] eg, 
			long timeoutMs) {
		long walIndex = 0; // TODO this should be set by a read
		MPaxPayload payload = new MPaxPayload(eg, walEntry, walIndex, timeoutMs);
		server.enqueue(payload, sendPrepareStage, payload);
		return new CommitFuture(payload);
	}
	
	class PaxosPrepareStage implements Stage<MPaxPayload> {
		// TODO this should use selected replica and not local replica
		Log logger = LogFactory.getLog(PaxosPrepareStage.class);
		 
		public NextAction<MPaxPayload> runStage(MPaxPayload payload) {
			// Assumes the chosen replica is up to date, which it will be if we 
			// just did a read.
			WALEntry existingEntry;
			payload.workingEntry.n = 1;
			try {
				existingEntry = wal.prepareLocal(payload.eg, payload.walIndex,
						payload.workingEntry.n);
			} catch(IOException e) {
				return new NextAction<MPaxPayload>(Action.FINISHED, null);
			}
			if(existingEntry != null && 
					existingEntry.n >= payload.requestedEntry.n) {
				payload.workingEntry = existingEntry;
				payload.usedOtherEntry = true;
			}

			Collection<ReplicaDesc> replicas = megalon.config.replicas.values();
			int numReplicas = replicas.size(); 
			payload.replResponses = new ReplResponses<MPaxPayload>(server, 
					sendAcceptStage, numReplicas-1, payload, false);
			
			List<ByteBuffer> outBytes = encodedPrepare(payload.eg, 
					payload.walIndex, payload.workingEntry.n);
			logger.debug("encodedPrepare gave" + RPCUtil.strBufs(outBytes));
			int numFailedReplicas = sendToRemoteReplicas(replicas, outBytes, payload);

			if(numFailedReplicas >= Util.quorumImpossible(numReplicas)) {
				// Enough replicas failed that quorum is impossible. Fail fast.
				return new NextAction<MPaxPayload>(Action.FINISHED, null);
			} else {
				return new NextAction<MPaxPayload>(Action.IGNORE, null);
			}
		}
		
		public int getNumConcurrent() {
			return 5; // TODO configure
		}

		public String getName() {
			return this.getClass().getName();
		}

		public int getBacklogSize() {
			return 10;
		}

		public void setServer(MultiStageServer<MPaxPayload> server) {}

		List<ByteBuffer> encodedPrepare(byte[] eg, long walIndex, long n) {
			AvroPrepare avroPrepare = new AvroPrepare();
			avroPrepare.walIndex = walIndex;
			avroPrepare.n = n;
			avroPrepare.entityGroup = ByteBuffer.wrap(eg);
			// TODO share/reuse/pool these objects, GC pressure
			ByteBufferOutputStream bbos = new ByteBufferOutputStream();
			bbos.write(MsgPrepare.MSG_ID);
			final DatumWriter<AvroPrepare> writer = 
				new SpecificDatumWriter<AvroPrepare>(AvroPrepare.class);
			Encoder enc = EncoderFactory.get().binaryEncoder(bbos, null);
			try {
				writer.write(avroPrepare, enc);
				enc.flush();
			} catch (IOException e) {
				throw new AssertionError(e);  // Can't happen
			}
			List<ByteBuffer> bbList = bbos.getBufferList();
			return bbList;
		}
	}
	
	class PaxosAcceptStage implements Stage<MPaxPayload> {
		Log logger = LogFactory.getLog(PaxosAcceptStage.class);
		
		public NextAction<MPaxPayload> runStage(MPaxPayload payload)
				throws Exception {
			// TODO pool/reuse to mitigate GC pressure
			// TODO move Avro details behind an interface
			DatumReader<AvroPrepareResponse> prepRespReader = 
				new SpecificDatumReader<AvroPrepareResponse>(AvroPrepareResponse.class);
			BinaryDecoder dec = null;
			
			// Track the "best known entry", starting with the local replica's
			// entry since we already have it.
			int numValidResponses = 1;
			
			Collection<ReplicaDesc> replicas = megalon.config.replicas.values();
			int numReplicas = replicas.size(); 
			
			// Look for the highest n among a quorum of responses. This is just
			// normal Paxos. We'll use this value in accept messages below.
			Set<Entry<String, List<ByteBuffer>>> responses = 
				payload.replResponses.getRemoteResponses().entrySet();
			AvroPrepareResponse avroPrepResp = new AvroPrepareResponse();
			for(Entry<String,List<ByteBuffer>> e: responses) {
				List<ByteBuffer> replicaBytes = e.getValue();
				if(replicaBytes == null) {
					logger.debug("Replica timed out: " + e.getKey());
					continue;
				}
				byte msgType = RPCUtil.extractByte(replicaBytes);
				logger.debug("Msg type is: " + msgType);
				assert msgType == MsgPrepareResp.MSG_ID;
				logger.debug("Good response from replica: " + e.getKey());
				
				numValidResponses++;
				logger.debug("Invoking decoder on: " + RPCUtil.strBufs(replicaBytes));
				InputStream is = new BBInputStream(replicaBytes);
				
				dec = DecoderFactory.get().binaryDecoder(is, dec);
				avroPrepResp = prepRespReader.read(avroPrepResp, dec);
				if(avroPrepResp.walEntry != null) {
					if(avroPrepResp.walEntry.n > payload.workingEntry.n) {
						payload.workingEntry = new WALEntry(avroPrepResp.walEntry);
						payload.usedOtherEntry = true;
						logger.debug("New best entry: " + payload.workingEntry);
					}
				}
			}
			int quorum = Util.quorum(numReplicas); // TODO replica set versions
			if(numValidResponses < quorum) {
				logger.debug("No quorum of prepare responses, failing. " + 
						"Quorum=" + quorum + ", saw " + numValidResponses);
				return new NextAction<MPaxPayload>(Action.FINISHED, null);
			}
			logger.debug("Quorum! Best entry among replicas:" + payload.workingEntry);
			List<ByteBuffer> outBytes = encodedAccept(payload.eg, 
					payload.workingEntry, payload.walIndex);
			
			payload.replResponses = new ReplResponses<MPaxPayload>(server, 
					respondStage, numReplicas, payload, true);
			
			// The local replica's response is kept separate from the others
			// for convenience of decoding/encoding avro from remote replicas.
			payload.replResponses.localResponse(wal.acceptLocal(payload.eg, 
					payload.walIndex, payload.workingEntry));
			
			int numFailedReplicas = sendToRemoteReplicas(replicas, outBytes, payload);
			if(numFailedReplicas >= Util.quorumImpossible(numReplicas)) {
				// Enough replicas failed that quorum is impossible. Fail fast.
				return new NextAction<MPaxPayload>(Action.FINISHED, null);
			} else {
				return new NextAction<MPaxPayload>(Action.IGNORE, null);
			}
		}
		
		List<ByteBuffer> encodedAccept(byte[] eg, WALEntry entry, long walIndex) {
			MsgAccept msgAccept = new MsgAccept(entry, walIndex, eg);
			AvroAccept avroAccept = msgAccept.toAvro();
			// TODO share/reuse/pool these objects, GC pressure
			ByteBufferOutputStream bbos = new ByteBufferOutputStream();
			bbos.write(MsgAccept.MSG_ID);
			final DatumWriter<AvroAccept> writer = 
				new SpecificDatumWriter<AvroAccept>(AvroAccept.class);
			Encoder enc = EncoderFactory.get().binaryEncoder(bbos, null);
			try {
				writer.write(avroAccept, enc);
				enc.flush();
			} catch (IOException e) {
				throw new AssertionError(e);  // Can't happen
			}
			List<ByteBuffer> bbList = bbos.getBufferList();
			return bbList;
		}

		public int getNumConcurrent() {
			return 2;
		}

		public String getName() {
			return this.getClass().getName();
		}

		public int getBacklogSize() {
			return 10;
		}

		public void setServer(MultiStageServer<MPaxPayload> server) {}
	}
	
	/**
	 * This stage will either restart the commit process (if a quorum of acks
	 * wasn't received for our accept), or return to the caller.
	 */
	class PaxosRespondStage implements Stage<MPaxPayload> {
		public static final int MAX_COMMIT_TRIES = 5; // TODO configurable

		Log logger = LogFactory.getLog(PaxosRespondStage.class);

		public NextAction<MPaxPayload> runStage(MPaxPayload payload)
				throws Exception {
			DatumReader<AvroAcceptResponse> acceptRespReader = 
				new SpecificDatumReader<AvroAcceptResponse>(AvroAcceptResponse.class);
			
			int numAcks = 0;
			Object localResponse = payload.replResponses.getLocalResponse(); 
			if(localResponse instanceof Boolean && (Boolean)localResponse) { 
				numAcks++;
			}
			
			Collection<ReplicaDesc> replicas = megalon.config.replicas.values();
			int numReplicas = replicas.size(); 

			// The remote replicas will ack our accept only if the value in our
			// accept was written to their commit log (or was already there).
			// If we get a quorum of acks, then the value we proposed was the
			// Paxos consensus value.
			Set<Entry<String, List<ByteBuffer>>> responses = 
				payload.replResponses.getRemoteResponses().entrySet();
			AvroAcceptResponse avroAcceptResp = new AvroAcceptResponse();
			BinaryDecoder dec = null;
			for(Entry<String,List<ByteBuffer>> e: responses) {
				List<ByteBuffer> replicaBytes = e.getValue();
				if(replicaBytes == null) {
					logger.debug("Replica timed out: " + e.getKey());
					continue;
				}
				byte msgType = RPCUtil.extractByte(replicaBytes);
				logger.debug("Msg type is: " + msgType);
				assert msgType == MsgAcceptResp.MSG_ID;
				
				logger.debug("Invoking decoder on: " + RPCUtil.strBufs(replicaBytes));
				InputStream is = new ByteBufferInputStream(replicaBytes);
				dec = DecoderFactory.get().binaryDecoder(is, dec);

				avroAcceptResp = acceptRespReader.read(avroAcceptResp, dec);
				if(avroAcceptResp.acked) {
					logger.debug("Ack response from replica: " + e.getKey());
					numAcks++;
				} else {
					logger.debug("Affirmative nack response from replica: " + 
							e.getKey());
				}
			}
			
			if(numAcks >= Util.quorum(numReplicas)) {
				logger.debug("Quorum of accept-responses!");
				payload.workingEntry.status = Status.CHOSEN;
				wal.putWAL(payload.eg, payload.walIndex, payload.workingEntry);
			} else {
				logger.debug("Accept-response quorum failed");
				if(payload.commitTries < MAX_COMMIT_TRIES) {
					logger.debug("Restarting commit process");
					return new NextAction<MPaxPayload>(Action.FORWARD, sendPrepareStage);
				} else {
					logger.debug("Too many commit retries, terminating");
				}
			}

			if(payload.usedOtherEntry) {
				logger.debug("We accepted someone else's value");
			} else {
				logger.debug("We accepted our own proposed value. Hooray");
				payload.committed = true;
			}
			return new NextAction<MPaxPayload>(Action.FINISHED, null);
		}

		public int getNumConcurrent() {
			return 3;
		}

		public String getName() {
			return this.getClass().getName();
		}

		public int getBacklogSize() {
			return 10;
		}

		public void setServer(MultiStageServer<MPaxPayload> server) {}
	}
	
	/**
	 * When a caller wants to asynchronously commit a transaction, this Future
	 * will inform them of the eventual completion or failure of the commit.
	 */
	public class CommitFuture implements Future<Boolean>, Finisher<MPaxPayload> {
		boolean committed;
		MPaxPayload payload;
		boolean done = false;
		
		protected CommitFuture(MPaxPayload payload) {
			this.payload = payload;
		}
		
		public boolean cancel(boolean arg0) {
			return false;
		}

		public Boolean get() throws InterruptedException, ExecutionException {
			payload.waitFinished();
			this.committed = payload.isCommitted();
			return committed;
		}

		public Boolean get(long duration, TimeUnit unit)
				throws InterruptedException, ExecutionException,
				TimeoutException {
			payload.waitFinished(duration, unit);
			return null;
		}

		public boolean isCancelled() {
			return false;
		}

		public boolean isDone() {
			return done;
		}

		public void finish(MPaxPayload payload) {
			this.committed = payload.committed;
			done = true;
		}
	}
	
	/**
	 * Send the given list of ByteBuffers to all replicas. Returns the
	 * number of replicas for which we don't have an open socket, which
	 * are failures. Other (reachable) replicas may still fail later.
	 */
	int sendToRemoteReplicas(Collection<ReplicaDesc> replicas, 
			List<ByteBuffer> outBytes, MPaxPayload payload) {
		int numFailedReplicas = 0;
		List<ByteBuffer> outBytesThisRepl;
		for(ReplicaDesc replicaDesc: replicas) {
			if(replicaDesc == megalon.config.myReplica) {
				// We assume the local replica was handled elsewhere.
				continue;
			}
			boolean aHostSucceeded = false;
			for(Host host: (List<Host>)Util.shuffled(replicaDesc.replsrv)) {
				outBytesThisRepl = RPCUtil.duplicateBufferList(outBytes);
				logger.debug("bytes this repl: " + RPCUtil.strBufs(outBytesThisRepl));
				RPCClient rpcCli = megalon.clientData.getReplSrvSocket(host);
				aHostSucceeded |= rpcCli.write(outBytesThisRepl, payload);
				if(!aHostSucceeded) {
					logger.debug("Host write failed: " + host);
				}
				break;
			}
			if(!aHostSucceeded) {
				numFailedReplicas++;
				logger.debug("All hosts for replica failed: " + replicaDesc);
			}
		}
		return numFailedReplicas;
	}
}
