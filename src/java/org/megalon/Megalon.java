package org.megalon;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Megalon {
	static Log logger = LogFactory.getLog(Megalon.class);
	public Config config;
	protected UUID uuid;
	public ReplServer replServ;
	public Coord coord;
	
//	static public final String REPL_OPT = "runreplsrv";
//	static public final String COORD_OPT = "runcoord";
//	static public final String CONF_OPT = "conf";

	public Megalon(Config config) {
		ReplServer replServ = null;
		Coord coord = null;
		
		this.config = config;
		this.uuid = UUID.randomUUID();
		
		if(config.run_replsrv) {
			replServ = new ReplServer(this);
			try {
				replServ.init();
			} catch (IOException e) {
				logger.error("ReplServer init exception", e);
			}
		}
		if(config.run_coord) {
			coord = new Coord(this);
		}
		this.replServ = replServ;
		this.coord = coord;
		
		if(config.run_replsrv && config.replsrv_listen) {
			replServ.startSocketAccepter();
		}
		if(config.run_coord && config.coord_listen) {
			coord.startSocketAccepter();
		}
	}
	
	public static void main(String[] args) {
//		Options options = new Options();
//		options.addOption(REPL_OPT, false, 
//				"Whether to run the replication server component");
//		options.addOption(COORD_OPT, false, 
//				"Whether to run the coordinator component");
//		options.addOption(CONF_OPT, true, 
//				"Where to find the megalon config file");
//		
//		CommandLineParser parser = new PosixParser();
//		CommandLine parsedArgs = null;
//		try {
//			parsedArgs = parser.parse(options, args);
//		} catch (ParseException e) {
//			e.printStackTrace(System.err);
//			System.exit(-1);
//		}
		
		String globalConfigPath = System.getProperty("megalon.conffile.global",
				"conf/megalon.yaml"); 
		String nodeConfigPath = System.getProperty("megalon.conffile.node", 
				"conf/node.yaml");
		String[] configFiles = new String[] {globalConfigPath, nodeConfigPath};
		Config config = new Config(configFiles);
		
		new Megalon(config);
		
		while(true) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {}
			
		}
	}
	
	
}
