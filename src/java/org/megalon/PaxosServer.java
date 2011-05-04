package org.megalon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config.Host;
import org.megalon.Config.ReplicaDesc;
import org.megalon.avro.AvroAccept;
import org.megalon.avro.AvroPrepare;
import org.megalon.avro.AvroPrepareResponse;
import org.megalon.messages.MegalonMsg;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.Finisher;
import org.megalon.multistageserver.MultiStageServer.NextAction;
import org.megalon.multistageserver.MultiStageServer.NextAction.Action;
import org.megalon.multistageserver.MultiStageServer.Stage;

public class PaxosServer {
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
		
		this.server = new MultiStageServer<MPaxPayload>(serverStages);
	}
	
	public Future<Boolean> commit(WALEntry walEntry, String eg, 
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
		 	String myReplica = megalon.config.myReplica.name;
			WALEntry existingEntry;
			payload.workingEntry.n = 1;
			try {
				existingEntry = wal.prepareLocal(payload.walIndex,
						payload.workingEntry.n);
			} catch(IOException e) {
				return new NextAction<MPaxPayload>(Action.FINISHED, null);
			}
			if(existingEntry != null && 
					existingEntry.n >= payload.requestedEntry.n) {
				payload.workingEntry = existingEntry;
				payload.usedExisting = true;
			}

			Collection<ReplicaDesc> replicas = megalon.config.replicas.values();
			int numReplicas = replicas.size(); 
			payload.replResponses.init(server, sendAcceptStage, numReplicas-1);
			
			List<ByteBuffer> outBytes = encodedPrepare(payload.walIndex,
					payload.workingEntry.n);
			logger.debug("encodedPrepare gave" + RPCUtil.strBufs(outBytes));
			RPCClient rpcCli;
			List<ByteBuffer> outBytesThisRepl;
			int numFailedReplicas = 0;
			for(ReplicaDesc replicaDesc: replicas) {
				if(replicaDesc == megalon.config.myReplica) {
					continue; // We already read the local replica
				}
				boolean aHostSucceeded = false;
				for(Host host: (List<Host>)Util.shuffled(replicaDesc.replsrv)) {
					outBytesThisRepl = duplicateBufferList(outBytes);
					logger.debug("bytes this repl: " + RPCUtil.strBufs(outBytesThisRepl));
					rpcCli = megalon.clientData.getReplSrvSocket(host);
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
			if(numFailedReplicas >= Util.quorumImpossible(numReplicas)) {
				// Enough replicas failed that quorum is impossible. Fail fast.
				return new NextAction<MPaxPayload>(Action.FINISHED, null);
			} else {
				return new NextAction<MPaxPayload>(Action.IGNORE, null);
			}
		}
		
//		long getNForEG(String entityGroup) {
//			return 0; // TODO for real
//		}

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

		List<ByteBuffer> duplicateBufferList(List<ByteBuffer> inList) {
			List<ByteBuffer> outList = new LinkedList<ByteBuffer>();
			for(ByteBuffer bb: inList) {
				outList.add(bb.duplicate());
			}
			return outList;
		}
		
		
		List<ByteBuffer> encodedPrepare(long walIndex, long n) {
			AvroPrepare avroPrepare = new AvroPrepare();
			avroPrepare.walIndex = walIndex;
			avroPrepare.n = n;
			// TODO share/reuse/pool these objects, GC pressure
			ByteBufferOutputStream bbos = new ByteBufferOutputStream();
			bbos.write(MegalonMsg.MSG_PREPARE);
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
	
	class NoopStage implements Stage<MPaxPayload> {
		public NextAction<MPaxPayload> runStage(MPaxPayload payload)
				throws Exception {
			throw new Exception("noop stage!");
		}

		public int getNumConcurrent() {
			return 2;
		}

		public String getName() {
			return "noop";
		}

		public int getBacklogSize() {
			return 5;
		}

		public void setServer(MultiStageServer<MPaxPayload> server) { }
	}
	
	class PaxosAcceptStage implements Stage<MPaxPayload> {
		public NextAction<MPaxPayload> runStage(MPaxPayload payload)
				throws Exception {
			// TODO pool/reuse
			final DatumReader<AvroPrepareResponse> prepRespReader = 
				new SpecificDatumReader<AvroPrepareResponse>(AvroPrepareResponse.class);
			Decoder dec;
			throw new IOException("Accept stage not coded");
			// Look for the highest n among a quorum of responses. This is just
			// normal Paxos. We'll use this value in accept messages below.
//			for(Entry<String,byte[]> e: payload.replResponses.responses.entrySet()) {
//				byte[] replicaBytes = e.getValue();
//				
//
//				// Each response is either a byte array (remote replica response)
//				// or a WALEntry (the local  
//				if(valThisReplica.getClass().isArray()) {
//					B
//					Decoder dec = DecoderFactory.get().binaryDecoder(payload.is, dec);
//					
//				}
//			}
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
	
	class PaxosRespondStage extends NoopStage {
		
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
}
