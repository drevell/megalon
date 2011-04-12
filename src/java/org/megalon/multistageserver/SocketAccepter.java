package org.megalon.multistageserver;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

/**
 * This class listens for incoming connections socket connections. When a new
 * connection is received, a new payload object is initialized and the request
 * will be send to a MultiStageServer for processing. To use this class, you
 * should inherit it and override the methods 
 * {@link SocketAccepter#makePayload} and  {@link SocketAccepter#finished} if
 * needed.
 * 
 * @param T is the class of the payload object that will be passed to/from the
 * stages of the MultiStageServer.
 */
public class SocketAccepter<T extends MultiStageServer.Payload> {
	MultiStageServer<T> server;
	InetAddress addr;
	int port;
	int startStage;
	Logger logger = Logger.getLogger(SocketAccepter.class);
	PayloadFactory<T> payloadFactory;
	boolean inited = false;
	
	public SocketAccepter(MultiStageServer<T> server, InetAddress addr,
			int port, int startStage, PayloadFactory<T> payloadFactory) {
		init(server, addr, port, startStage, payloadFactory);
	}
	
	public SocketAccepter() { }
	
	public void init(MultiStageServer<T> server, InetAddress addr, int port,
			int startStage, PayloadFactory<T> payloadFactory) {
		this.server = server;
		this.addr = addr;
		this.port = port;
		this.startStage = startStage;
		this.payloadFactory = payloadFactory;
		this.inited = true;
	}
	
	public interface PayloadFactory<T extends MultiStageServer.Payload> {
		public T makePayload(SocketChannel sockChan);
	}
	
	public void finished(T payload) {
		close(payload.sockChan);
	}
	
	public void close(SocketChannel sockChan) {
		try {
			sockChan.close();
		} catch (IOException e) {
			logger.warn("Exception while closing socket", e);
		}
	}
	
	public void runForever() throws IOException, Unconfigured {
		if(!inited) {
			throw new Unconfigured("SocketAccepter.runForever invoked " +
					"before initialized");
		}
		// Bind to the desired IP/port
		ServerSocketChannel servChan = ServerSocketChannel.open();
		servChan.socket().bind(new InetSocketAddress(addr, port));
		
		while(true) {
			// TODO multiple accepter threads?
			SocketChannel acceptedChan = servChan.accept();
			InetSocketAddress remoteAddr = (InetSocketAddress)
			   acceptedChan.socket().getRemoteSocketAddress();
			logger.debug("New connection from " + remoteAddr);
			
			T payload = payloadFactory.makePayload(acceptedChan);
			
			if(!server.enqueue(payload, startStage)) {
				logger.warn("The start stage (id " + startStage
						+ ") wasn't defined");
			}
		}
	}
}
