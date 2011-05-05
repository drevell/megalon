package org.megalon.multistageserver;

import java.io.IOException;
import java.net.InetAddress;

import org.megalon.multistageserver.SocketAccepter.PayloadFactory;

/**
 * This class makes it easy to create a socket server. It assumes you already
 * have some server that implements the core server logic. Just pass some
 * information about your server to the constructor, and it will be called when
 * there's a new socket connection. */
public class SocketServer<T extends SocketPayload> extends MultiStageServer<T> {
	SocketAccepter<T> accepter;
	SelectorStage<T> selectorStage;
	ProxyStage proxyStage;
	
	public SocketServer(MultiStageServer<T> serverToProxy, Stage<T> entryStage,
			InetAddress addr, int port, PayloadFactory<T> payloadFactory, 
			boolean blocking) throws IOException {
		proxyStage = new ProxyStage();
		selectorStage = new SelectorStage<T>(proxyStage, "socketServerSelector",
				2, 50);
		accepter = new SocketAccepter<T>(serverToProxy, addr, port, selectorStage,
				payloadFactory, blocking);
	}
	
	class ProxyStage implements Stage<T> {
		public org.megalon.multistageserver.MultiStageServer.NextAction<T> runStage(
				T payload) throws Exception {
			return null;
		}

		public int getNumConcurrent() {
			return 1;
		}

		public String getName() {
			return "socketServerProxyStage";
		}

		public int getBacklogSize() {
			return 50;
		}

		public void setServer(MultiStageServer<T> server) {}
	}
}
