package org.megalon;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

public class Main {
	static Logger logger = Logger.getLogger(Main.class);
	static public final String REPL_OPT = "runreplsrv";
	static public final String COORD_OPT = "runcoord";
	static public final String CONF_OPT = "conf";
	
	public static void main(String[] args) {
		Options options = new Options();
		options.addOption(REPL_OPT, false, 
				"Whether to run the replication server component");
		options.addOption(COORD_OPT, false, 
				"Whether to run the coordinator component");
		options.addOption(CONF_OPT, true, 
				"Where to find the megalon config file");
		
		CommandLineParser parser = new PosixParser();
		CommandLine parsedArgs = null;
		try {
			parsedArgs = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace(System.err);
			System.exit(-1);
		}
		
		String confFileName = parsedArgs.getOptionValue("repl", "conf/megalon.yaml");
		Config config = new Config(confFileName);
		
		ReplServer replServ;
//		Coordinator coord;
		
		if(config.run_replsrv) {
			replServ = new ReplServer(config);
			replServ.start();
		}
		
		while(true) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {}
			
		}
	}
	
	
}
