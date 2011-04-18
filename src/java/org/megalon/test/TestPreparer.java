package org.megalon.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config;
import org.megalon.Megalon;
import org.megalon.PaxosProposer;
import org.megalon.SingleWrite;
import org.megalon.WALEntry;

public class TestPreparer {
	public static void main(String[] args) throws Exception {
		Log logger = LogFactory.getLog(TestPreparer.class);
		
		long walIndex = Long.parseLong(args[0]);
		long n = Long.parseLong(args[1]);
		byte[] value = args[2].getBytes();
		
		String globalConfigPath = System.getProperty("megalon.conffile.global",
				"conf/megalon.yaml"); 
		String nodeConfigPath = System.getProperty("megalon.conffile.node", 
				"conf/node.yaml");
		String[] configFiles = new String[] {globalConfigPath, nodeConfigPath};
		Config config = new Config(configFiles);

		Megalon megalon = new Megalon(config);
		PaxosProposer proposer = new PaxosProposer(megalon);
		while(!megalon.replServ.isReady()) {
			logger.warn("replServ unready, sleeping");
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {}
		}
		WALEntry walEntry = proposer.prepare(walIndex, n, 1000);
		if(walEntry == null) {
			logger.info("null==ACK, proceeding with accept");
			SingleWrite write = new SingleWrite("table!".getBytes(), 
					"cf!".getBytes(), "col!".getBytes(), value);
			List<SingleWrite> values = new ArrayList<SingleWrite>(1);
			values.add(write);
			walEntry = new WALEntry(n, values, WALEntry.Status.ACCEPTED);
		} else {
			logger.info("non-null==NACK, accepting old value");
		}
		logger.info("Accepting WALEntry: " + walEntry);
		proposer.accept(walIndex, walEntry, 1000);
	}
}
