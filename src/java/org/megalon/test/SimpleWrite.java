package org.megalon.test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.megalon.Client;
import org.megalon.Config;
import org.megalon.Megalon;

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
		Client proposer = new Client(megalon);
		Thread.sleep(1000);
		
		while(true) {
			if(megalon.getReplServ().isReady()) {
				break;
			}
			logger.debug("ReplServer unready...");
			Thread.sleep(1000);
		}
		Thread.sleep(1000); // wait for init to finish

		byte[] table = "MyTable".getBytes();
		byte[] row = "MyRow".getBytes();
		byte[] cf = "MyCF".getBytes();
		byte[] qual = "MyQual".getBytes();
		byte[] val = "MyVal".getBytes();
		
		Put put = new Put(row);
		put.add(cf, qual, val);
		List<Row> opBatch = new LinkedList<Row>();
		opBatch.add(put);
		Object[] results = new Object[1];
		proposer.batch(table, opBatch, results);
		assert results[0] == null;
		
		if(proposer.commitSync("myentitygroup!".getBytes(), 1000)) {
			logger.info("Transaction committed!");
		} else {
			logger.info("Transaction failed to commit!");
		}

		Get get = new Get(row);
		get.addColumn(cf, qual);
		opBatch.clear();
		opBatch.add(get);
		proposer.batch(table, opBatch, results);
		
		assert results[0] instanceof Result;
		Result result = (Result)results[0];
		logger.debug("Result is: " + result);
		assert Arrays.equals(result.getValue(cf, qual), val);
		
		logger.info("Done");
	}
}
