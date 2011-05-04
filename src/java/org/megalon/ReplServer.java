package org.megalon;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.Stage;
import org.megalon.multistageserver.SelectorStage;
import org.megalon.multistageserver.SocketAccepter;
import org.megalon.multistageserver.SocketPayload;

/**
 * This class implements the Replication Server component of megalon. It
 * does the same things as the Replication Server in Megastore (Paxos accepter,
 * apply changes to the WAL & main DB).
 */
public class ReplServer {
	static Log logger = LogFactory.getLog(ReplServer.class);
	Megalon megalon;
	InetSocketAddress proxyTo;
	WAL wal;
	MultiStageServer<MReplPayload> coreServer;
	final SocketAccepter<MSocketPayload> socketAccepter = 
		new SocketAccepter<MSocketPayload>();
	MultiStageServer<MSocketPayload> socketServer;
	boolean ready = false;
	Stage<MReplPayload> execStage;
	Stage<MSocketPayload> selectorStage;
	AvroDecodeStage avroDecodeStage; 
	AvroEncodeStage avroEncodeStage; 
	PaxosProposer paxos;
	
	public ReplServer(Megalon megalon) throws IOException {
		this.megalon = megalon;
	}
	
	public void init() throws IOException {
		logger.debug("Replication server init'ing");
		wal = new WAL(megalon);

		// The coreServer contains the stages that are the same regardless of 
		// whether the request arrived by socket or by function call.
		Set<Stage<MReplPayload>> coreStages = new HashSet<Stage<MReplPayload>>();
		execStage = new ReplRemoteHandlerStage(wal);
		coreStages.add(execStage);
		coreServer = new MultiStageServer<MReplPayload>(coreStages);
		
		// The socketServer contains the stages that only run for socket
		// connections. The socketServer hands off requests to coreServer to
		// do the actual database operations.
		Set<Stage<MSocketPayload>> socketSvrStages = 
			new HashSet<Stage<MSocketPayload>>();
		avroDecodeStage = new AvroDecodeStage();
		selectorStage = new SelectorStage<MSocketPayload>(avroDecodeStage, 
				"replSelectorStage", 1, 50);
		avroEncodeStage = new AvroEncodeStage(selectorStage);
		avroDecodeStage.init(avroEncodeStage, coreServer, 
				execStage, selectorStage);
		socketSvrStages.add(selectorStage);
		socketSvrStages.add(avroEncodeStage);
		socketSvrStages.add(avroDecodeStage);
		socketServer = new MultiStageServer<MSocketPayload>(socketSvrStages);
		
		paxos = new PaxosProposer(megalon); 

		
		ready = true; // TODO use this value to prevent premature ops?
		logger.debug("Replication server done with init");
	}
	
	protected void startSocketAccepter() {
		// The socket accepter will send new connections to the socketServer's
		// stage "selectorStage".
		socketAccepter.init(socketServer, null, megalon.config.replsrv_port, 
				selectorStage, new MSocketPayload.Factory(), false);
		Thread accepterThread = new Thread() {
			public void run() {
				try {
					logger.debug("MSocketAccepter starting on port " + 
							megalon.config.replsrv_port);
					socketAccepter.runForever();
				} catch (Exception e) {
					logger.warn("ReplServer accepter exception", e);
				}
			}
		};
		accepterThread.setDaemon(true);
		accepterThread.start();
		logger.debug("Replication server accepter thread running");
	}
	
	public boolean isReady() {
		return ready;
	}
	
	public void close(SocketPayload sockPayload) {
		try {
			sockPayload.sockChan.close();
		} catch (IOException e) {
			logger.info("IOException closing socket", e);
		}
	}
}
