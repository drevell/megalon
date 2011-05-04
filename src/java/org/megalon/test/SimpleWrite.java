package org.megalon.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config;
import org.megalon.Megalon;
import org.megalon.PaxosProposer;

public class SimpleWrite {
	public static void main(String[] args) throws Exception {
		Log logger = LogFactory.getLog(SimpleWrite.class);
		
		String confDir = System.getProperty("megalon.confdir", "conf");
		logger.debug("confDir is " + confDir);
		String globalConfigPath = System.getProperty("megalon.conffile.global",
				confDir + "/megalon.yaml"); 
		String nodeConfigPath = System.getProperty("megalon.conffile.node", 
				confDir + "/node.yaml");
		String[] configFiles = new String[] {globalConfigPath, nodeConfigPath};
		Config config = new Config(configFiles);

		Megalon megalon = new Megalon(config);
		PaxosProposer proposer = new PaxosProposer(megalon);
		Thread.sleep(1000);
		
		while(true) {
			if(megalon.getReplServ().isReady()) {
				break;
			}
			logger.debug("ReplServer unready...");
			Thread.sleep(1000);
		}
		Thread.sleep(1000);
		proposer.write("table!".getBytes(), "cf!".getBytes(), "col!".getBytes(),
				"value!".getBytes());
		if(proposer.commitSync("myentitygroup!", 1000)) {
			logger.info("Transaction committed!");
		} else {
			logger.info("Transaction failed to commit!");
		}
	}
}
