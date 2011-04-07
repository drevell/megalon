package org.megalon.multistageserver;

import java.net.InetAddress;
import java.nio.channels.SocketChannel;

import org.megalon.multistageserver.MultiStageServer.Payload;
import org.megalon.multistageserver.SocketAccepter.PayloadFactory;


/**
 * For users who don't need a custom Payload type to pass between the stages,
 * this class saves the effort of inheriting SocketAccepter.
 */

public class PlainSocketAccepter extends SocketAccepter<Payload> implements 
PayloadFactory<Payload> {
	public PlainSocketAccepter(MultiStageServer<Payload> server, 
			InetAddress addr, int port, int startStage) {
		super.init(server, addr, port, startStage, this);
	}
	
	public Payload makePayload(SocketChannel sockChan) {
		return new Payload(sockChan);
	}
}
