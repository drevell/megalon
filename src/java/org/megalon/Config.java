package org.megalon;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.scanner.ScannerException;

/**
 * This class processes the raw nested map from the YAML parser into a friendly
 * object that can be used by other parts of the program. It handles details
 * like setting defaults, making sure that certain values are present, and
 * making sure all config values have the correct type. 
 */
public class Config {
	Log logger = LogFactory.getLog(Config.class);
	
	String zk_path = "/megalon";
	boolean run_replsrv = false;
	boolean replsrv_listen = false;
	int replsrv_port = 35792;
	boolean run_coord = false;
	boolean coord_listen = false;
	int coord_port = 35791;
	
	ReplicaDesc myReplica;
	Map<String,ReplicaDesc> replicas;
	
	public class Host {
		String nameOrAddr;
		int port;
		
		public Host(String nameOrAddr, int port) {
			this.nameOrAddr = nameOrAddr;
			this.port = port;
		}
	}
	
	/**
	 * Parse the configuration from the given YAML file.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Config(String[] confFileNames) {
		File[] confFiles = new File[confFileNames.length];
		for(int i=0; i<confFileNames.length; i++) {
			confFiles[i] = new File(confFileNames[i]);
		}
		logger.debug("Config files:" + Arrays.toString(confFileNames));
		
		// Process the YAML config file into nested Collections objects
		Map<String, Object> yamlConf = new HashMap<String, Object>();
		for(File file: confFiles) {
			logger.debug("Loading config file at " + file.getPath());
			try {
				FileInputStream fis = new FileInputStream(file);
				Map<String, Object> thisFileYaml = 
					(Map<String, Object>)new Yaml().load(fis);
				yamlConf.putAll(thisFileYaml);
			} catch (FileNotFoundException e) {
				fail("Couldn't open config file at " + file.getPath());
			} catch (ClassCastException e) {
				fail("Config seemed to be grossly misformatted.");
			} catch (ScannerException e) {
				fail("YAML parse failed, exception " + e.getClass().getName(), e);
			}
		}
		
		// The config contains a top-level section named "global"
		Map<String,Object> globalConf = (Map<String,Object>)safeGet(yamlConf, 
				Map.class, "global", 
				"Config should have a top-level section \"%k\"" , null);

		zk_path = (String)safeGet(globalConf, String.class, "zk_base_path", 
				"config global section should have a %t \"%k\"", zk_path);

		// The global section contains a list of replica descriptions
		replicas = new HashMap<String,ReplicaDesc>();
		List replList = (List)safeGet(globalConf, List.class, "replicas", 
				"config global section should have a value \"%k\" giving a " +
				" sequence of replica descriptions", null);
		for(Object o: replList) {
			if(!(o instanceof Map)) {
				fail("Replica list was misformatted");
			}
			Map<String,Object> oneReplicaMap = (Map<String,Object>)o;
			
			String replName = (String)safeGet(oneReplicaMap, String.class, 
					"name", "each replica description needs a string \"%k\"",
					null);

			List hbaseServerStrings = (List)safeGet(oneReplicaMap, List.class, 
					"hbase", "Each replica description should have a list of " +
					"server host:port descriptions under the key \"%k\"", null);
			List<Host> hbaseServers = parseHostList(hbaseServerStrings);
			
			List coordServerStrings = (List)safeGet(oneReplicaMap, List.class, 
					"coord", "Each replica description should have a list of " +
					"server host:port descriptions under the key \"%k\"", null);
			List<Host> coordServers = parseHostList(coordServerStrings);
			
			List replServerStrings = (List)safeGet(oneReplicaMap, List.class, 
					"replsrv", "Each replica description should have a list of " +
					"server host:port descriptions under the key \"%k\"", null);
			List<Host> replServers = parseHostList(replServerStrings);
			
			List zkServerStrings = (List)safeGet(oneReplicaMap, List.class, 
					"zk", "Each replica description should have a list of " +
					"server host:port descriptions under the key \"%k\"", null);
			List<Host> zkServers = parseHostList(zkServerStrings);
			
			ReplicaDesc replicaDesc = new ReplicaDesc(replName, hbaseServers,
					coordServers, replServers, zkServers);
			replicas.put(replName, replicaDesc);
		}
		
		// There's a top-level config section named "node" with this node's conf
		Map<String,Object> nodeConf = (Map<String,Object>)safeGet(yamlConf,
				Map.class, "node", "config should have a top-level section " +
				"named \"%k\"", null);
		
		// Set up a shortcut to the local replica descriptor 
		String replicaName = (String)safeGet(nodeConf, String.class, 
				"repl_name", "node config should have a string \"%k\"", null);		
		myReplica = replicas.get(replicaName);
		if(myReplica == null) {
			fail("This node is a member of replica \"" + replicaName +
					" but no such replica was configured");
		}
		
		run_replsrv = (Boolean)safeGet(nodeConf, Boolean.class, "run_replsrv", 
				"node config should have a boolean \"%k\"", false);
		replsrv_listen = (Boolean)safeGet(nodeConf, Boolean.class, 
				"replsrv_listen", "node config should have a boolean \"%k\"", 
				false);
		replsrv_port = (Integer)safeGet(nodeConf, Integer.class, "replsrv_port",
				"node config \"%k\" should have type \"%t\"", 35792);
		run_coord = (Boolean)safeGet(nodeConf, Boolean.class, "run_coord", 
				"node config should have a boolean \"%k\"", run_coord);
		coord_listen = (Boolean)safeGet(nodeConf, Boolean.class, 
				"coord_listen", "node config should have a boolean \"%k\"", 
				false);
		coord_port = (Integer)safeGet(nodeConf, Integer.class, "coord_port",
				"node config \"%k\" should have type \"%t\"", coord_port);
		
	}
	
	/**
	 * Given a string in the form 1.2.3.4:5000, 1:2:3:4:5:6:7:8::5000, or
	 * hostname:5000, return a Host.
	 * 
	 */
	protected Host parseAddress(String addrAndPort) {
		String[] addrAndPortArray;
		if(addrAndPort.contains("::")) {
			// The format is IPv6, addr1:addr2:...::port
			addrAndPortArray = addrAndPort.split("::");
		} else {
			// The format is not IPv6, so should be host:port or 1.2.3.4:port
			addrAndPortArray = addrAndPort.split(":");
		}

		if(addrAndPortArray.length != 2) {
			fail("Address in config is malformed: " + addrAndPort);
		}
		String host = addrAndPortArray[0];
		String portString = addrAndPortArray[1];
		int port;
		try {
			port = Integer.parseInt(portString);
		} catch (NumberFormatException e) {
			fail("Invalid port: " + portString);
			return null; // Just to shut up the compiler. Unreachable.
		}
		return new Host(host, port);
	}
	
	protected class ReplicaDesc {
		String name;
		List<Host> hbase;
		List<Host> coord;
		List<Host> replsrv;
		List<Host> zk;
		public ReplicaDesc(String name, List<Host> hbase, List<Host> coord,
				List<Host> replsrv, List<Host> zk) {
			super();
			this.name = name;
			this.hbase = hbase;
			this.coord = coord;
			this.replsrv = replsrv;
			this.zk = zk;
		}
	}
	
	List<String> parseStringList(List<Object> l) {
		List<String> strings = new LinkedList<String>();
		for(Object o: l) {
			if(!(o instanceof String)) {
				fail("Config expected a string, saw " + o.toString());
			}
			strings.add((String)o);
		}
		return strings;
	}
	
	List<Host> parseHostList(List<Object> l) {
		List<String> strings = parseStringList(l);
		List<Host> hosts = new LinkedList<Host>();
		
		for(String s: strings) {
			hosts.add(parseAddress(s));
		}
		return hosts;
	}
	
	
	void fail(String msg) {
		fail(msg, null);
	}
	
	void fail(String msg, Throwable e) {
		logger.error(msg, null);
		System.exit(-1);
	}
	
	/**
	 * To avoid duplicating code while parsing the configuration, this function
	 * is a type-safe way of extracting a value from a Map<String,Object>. It
	 * makes sure the extracted object has the expected type, and it can fill
	 * in a default value if desired. If the given defaultVal is null, we assume
	 * that the value is required, so we exit the JVM.
	 */
	Object safeGet(Map<String, Object> m, @SuppressWarnings("rawtypes") Class c,
			String key, String errMsg, Object defaultVal) {
		
		errMsg = errMsg.replace("%k", key).replace("%t", c.getName());
		Object valForKey = m.get(key); 
		
		if(valForKey == null) {
			if(defaultVal == null) {
				fail(errMsg);
			} else {
				return c.cast(defaultVal);
			}
		}
		
		try {
			return c.cast(m.get(key));
		} catch (ClassCastException e) {
			fail(errMsg);
			return null; // Unreachable, compiler whines
		}
	}
}
