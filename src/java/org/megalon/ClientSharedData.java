package org.megalon;

import java.util.HashMap;
import java.util.Map;

import org.megalon.Config.Host;
import org.megalon.Config.ReplicaDesc;

/**
 * This is a singleton class is instantiated if config value "run_client" is 
 * true. It contains data that is shared between clients in a single JVM. For
 * example, if there are two threads in the same JVM writing to the same Megalon
 * database, we want them to share TCP connections to replication servers.
 */
public class ClientSharedData {
	Map<Host,RPCClient> replServerSockets = 
		new HashMap<Host,RPCClient>();
	Map<String,RPCClient> coordSockets = new HashMap<String,RPCClient>();
	Megalon megalon;
	
	public ClientSharedData(Megalon megalon) {
		this.megalon = megalon;
		// TODO these sockets should be opened in the background
		updateReplServerSockets();
		updateCoordinatorSockets();
	}
	
	/**
	 * For each replication server that we know about (except this replica),
	 * create a connection (a RPCClient).
	 */
	protected void updateReplServerSockets() {
		for(ReplicaDesc replDesc: megalon.config.replicas.values()) {
			if(replDesc == megalon.config.myReplica) {
				continue;
			}
			for(Host host: replDesc.replsrv) {
				if(!replServerSockets.containsKey(host)) {
					replServerSockets.put(host, 
							new RPCClient(host, replDesc.name));
				}
			}
		}
	}
	
	/**
	 * For each coordinator that we know about, create a connection (RPCClient).
	 */
	protected void updateCoordinatorSockets() {
		for(ReplicaDesc replDesc: megalon.config.replicas.values()) {
			if(replDesc == megalon.config.myReplica && megalon.config.run_coord) {
				// Don't connect to local DC if we there's a coordinator in our JVM 
				continue;
			}
			assert replDesc.coord.size() == 1; // for now, no clustered coordinators
			Host host = replDesc.coord.get(0);
			coordSockets.put(replDesc.name, new RPCClient(host, replDesc.name));
			
//			for(Host host: replDesc.coord) {
//				if(!coordSockets.containsKey(host)) {
//					coordSockets.put(host, new RPCClient(host, replDesc.name));
//				}
//			}
		}
	}
	
	/**
	 * Get the RPCClient that is connected to a certain remote
	 * replication server.
	 */
	public RPCClient getReplSrvSocket(Host host) {
		return replServerSockets.get(host);
	}
}
