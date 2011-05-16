package org.megalon;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config.ReplicaDesc;

public class CoordTest {
	public static void main(String[] args) throws Exception {
		Log logger = LogFactory.getLog(CoordTest.class);
		
		String confDir = System.getProperty("megalon.confdir", "conf");
		logger.debug("confDir is " + confDir);
		String globalConfigPath = System.getProperty("megalon.conffile.global",
				confDir + "/megalon.yaml"); 
		String nodeConfigPath = System.getProperty("megalon.conffile.node", 
				confDir + "/node.yaml");
		String[] configFiles = new String[] {globalConfigPath, nodeConfigPath};
		Config config = new Config(configFiles);

		Megalon megalon = new Megalon(config);
		
		try {
			Thread.sleep(1000); // wait for things to init
		} catch (InterruptedException e) {}
		
		byte[] eg = "my_eg".getBytes();
		
		CoordClient c = new CoordClient(megalon);
		assert c.checkValidSync("west", eg, 1000) == false;
		logger.debug("west checkvalid succeeded");
		assert c.checkValidSync("east", eg, 1000) == false;
		logger.debug("east checkvalid succeeded");
		
		List<ReplicaDesc> replicas = new LinkedList<ReplicaDesc>();
		replicas.addAll(megalon.getConfig().replicas.values());
		
		Map<String,Boolean> validatedEgs = c.validateSync(replicas, eg, 1, 1000);
		
		for(Entry<String,Boolean> e: validatedEgs.entrySet()) {
			assert e.getValue(): e.getKey() + " failed validation";
		}
		assert replicas.size() == validatedEgs.size();
		
		// TODO test async versions also
	}
}
