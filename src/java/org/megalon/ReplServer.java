package org.megalon;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.StageDesc;
import org.megalon.multistageserver.SocketAccepter;

public class ReplServer extends Thread {
	public static final int CONCURRENT_CLIENTS = 1000;
	public static final int STAGE_ID = 1;
	static Logger logger = Logger.getLogger(ReplServer.class);
	Config conf;
	
	public ReplServer(Config conf) {
		this.conf = conf;
	}
	
	public void run() {
		logger.debug("Replication server starting");
		
		Map<Integer,StageDesc<MPayload>> stages = 
			new HashMap<Integer,StageDesc<MPayload>>();
		StageDesc<MPayload> stageDesc = new StageDesc<MPayload>(CONCURRENT_CLIENTS, 
				new ReplServerStage(), "repl_server");
		stages.put(STAGE_ID, stageDesc);
		MultiStageServer<MPayload> server = 
			new MultiStageServer<MPayload>(stages, logger);
		SocketAccepter<MPayload> accepter = new MSocketAccepter(server,
				null, conf.replsrv_port, STAGE_ID);
		try {
			accepter.runForever();
		} catch(Exception e) {
			logger.warn("Repl server had an Exception!", e);
		}
	}
	
	class ReqState {
		 
	}
	
	class ReplServerStage implements MultiStageServer.Stage<MPayload> {
		@Override
		public int runStage(MPayload payload) throws Exception {
			return 0;
		}
	}
	
	public static class ReplServerStatus {
		
	}
}
