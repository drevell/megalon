package org.megalon;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.ReqMetaData;
import org.megalon.multistageserver.MultiStageServer.Stage;
import org.megalon.multistageserver.MultiStageServer.StageDesc;

public class ReplServer extends Thread {
	public static final int CONCURRENT_CLIENTS = 1000;
	static Logger logger = Logger.getLogger(ReplServer.class);
	Config conf;
	
	public ReplServer(Config conf) {
		this.conf = conf;
	}
	
	public void run() {
		logger.debug("Replication server starting");
//		logger.debug("My yaml is: " + yaml.toString() + ", type: " + 
//				yaml.getClass().getName());
		
		MultiStageServer server = new MultiStageServer(logger);
		StageDesc stageDesc = new StageDesc(CONCURRENT_CLIENTS, 
				new ReplServerStage(), "repl_server");
		Map<Integer,StageDesc> stages = new HashMap<Integer,StageDesc>();
		stages.put(0, stageDesc);
		
		try {
			server.runForever(null, conf.replsrv_port, stages, 0);
		} catch(IOException e) {
			logger.warn("Repl server had an IOException!", e);
		}
	}
	
	class ReplServerStage implements MultiStageServer.Stage {
		@Override
		public int runStage(ReqMetaData work) throws Exception {
			
			return 0;
		}
	}
}
