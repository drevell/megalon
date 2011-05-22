package org.megalon.multistageserver;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.multistageserver.MultiStageServer.Finisher;
import org.megalon.multistageserver.MultiStageServer.Stage;

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
public class SocketAccepter<T extends SocketPayload> {
	MultiStageServer<T> server;
	InetAddress addr;
	int port;
	Stage<T> startStage;
	Log logger = LogFactory.getLog(SocketAccepter.class);
	PayloadFactory<T> payloadFactory;
	boolean inited = false;
	boolean blocking;
	
	Finisher<T> socketCloseFinisher = new Finisher<T>() {
		public void finish(T payload) {
			logger.debug("SocketAccepter finisher closing socket");
			T sockPayload = (T)payload;
			try {
				sockPayload.sockChan.close();
			} catch (IOException e) {
				logger.debug("IOException closing socket", e);
			}
		}
	};
	
	public SocketAccepter(MultiStageServer<T> server, InetAddress addr,
			int port, Stage<T> startStage, PayloadFactory<T> payloadFactory, 
			boolean blocking) {
		init(server, addr, port, startStage, payloadFactory, blocking);
	}
	
	public SocketAccepter() { }
	
	public void init(MultiStageServer<T> server, InetAddress addr, int port,
			Stage<T> startStage, PayloadFactory<T> payloadFactory, 
			boolean blocking) {
		this.server = server;
		this.addr = addr;
		this.port = port;
		this.startStage = startStage;
		this.payloadFactory = payloadFactory;
		this.blocking = blocking;
		this.inited = true;
	}
	
	public interface PayloadFactory<T extends SocketPayload> {
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
		try {
			if(!inited) {
				throw new Unconfigured("SocketAccepter.runForever invoked " +
						"before initialized");
			}
			// Bind to the desired IP/port
			ServerSocketChannel servChan = ServerSocketChannel.open();
			servChan.socket().bind(new InetSocketAddress(addr, port));
		
			while(true) {
				//logger.debug("To accept()");
				SocketChannel acceptedChan = servChan.accept();
				InetSocketAddress remoteAddr = (InetSocketAddress)
					acceptedChan.socket().getRemoteSocketAddress();
				logger.debug("New connection from " + remoteAddr);
				acceptedChan.configureBlocking(blocking);
				
				T payload = payloadFactory.makePayload(acceptedChan);
				
				if(!server.enqueue(payload, startStage, socketCloseFinisher)) {
					logger.warn("The start stage (id " + startStage
							+ ") wasn't defined");
				}
			}
		} catch (IOException e) {
			logger.warn("SocketAccepter exception", e);
			throw e;
		}
	}
}
