package org.megalon;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.Config.Host;

public class Megalon {
	protected Config config;
	protected ReplServer replServ = null;
	protected Coord coord = null;
	protected ClientSharedData clientData = null;
	protected PaxosServer paxosServer = null;
	
	public static final String OPT_NODAEMON = "nodaemon";
	
	static Log logger = LogFactory.getLog(Megalon.class);
	protected UUID uuid;
	protected Map<Host,PaxosSocketMultiplexer> replSrvSockets =
		new HashMap<Host,PaxosSocketMultiplexer>();
	
	public Megalon(Config config) throws IOException {
		ReplServer replServ = null;
		Coord coord = null;
		
		this.config = config;
		this.uuid = UUID.randomUUID();
		
		if(config.run_replsrv) {
			try {
				replServ = new ReplServer(this);
				replServ.init();
			} catch (IOException e) {
				logger.error("ReplServer init exception", e);
				throw e;
			}
		}
		if(config.run_coord) {
			coord = new Coord(this);
		}
		if(config.run_client) {
			clientData = new ClientSharedData(this);
			paxosServer = new PaxosServer(this);
		}
		
		this.replServ = replServ;
		this.coord = coord;
		
		if(config.run_replsrv && config.replsrv_listen) {
			logger.debug("Repl server options both true");
			replServ.startSocketAccepter();
		}
		if(config.run_coord && config.coord_listen) {
			coord.startSocketAccepter();
		}
	}
	
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OPT_NODAEMON, false, "Run in the foreground");
//		options.addOption(REPL_OPT, false, 
//				"Whether to run the replication server component");
//		options.addOption(COORD_OPT, false, 
//				"Whether to run the coordinator component");
//		options.addOption(CONF_OPT, true, 
//				"Where to find the megalon config file");
//		
		CommandLineParser parser = new PosixParser();
		CommandLine parsedArgs = null;
		try {
			parsedArgs = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace(System.err);
			System.exit(-1);
		}
		
		String confDir = System.getProperty("megalon.confdir", "conf");
		logger.debug("confDir is " + confDir);
		String globalConfigPath = System.getProperty("megalon.conffile.global",
				confDir + "/megalon.yaml"); 
		String nodeConfigPath = System.getProperty("megalon.conffile.node", 
				confDir + "/node.yaml");
		String[] configFiles = new String[] {globalConfigPath, nodeConfigPath};
		Config config = new Config(configFiles);
		
//		if(!parsedArgs.hasOption(OPT_NODAEMON)) {
//			logger.debug("Running in background, closing stdout & stderr");
//			System.out.close();
//			System.err.close();
//		}
		new Megalon(config);
		while(true) {
			try {
				Thread.sleep(100000);
			} catch (InterruptedException e) {}
		}
	}
	
	public Config getConfig() {
		return config;
	}

	public ReplServer getReplServ() {
		return replServ;
	}

	public Coord getCoord() {
		return coord;
	}

	public Map<Host, PaxosSocketMultiplexer> getReplSrvSockets() {
		return replSrvSockets;
	}


}
