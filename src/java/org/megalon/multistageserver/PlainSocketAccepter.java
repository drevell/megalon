package org.megalon.multistageserver;

import java.net.InetAddress;
import java.nio.channels.SocketChannel;

import org.megalon.multistageserver.MultiStageServer.Stage;
import org.megalon.multistageserver.SocketAccepter.PayloadFactory;

/**
 * For users who don't need a custom Payload type to pass between the stages,
 * this class saves the effort of inheriting SocketAccepter.
 */

public class PlainSocketAccepter extends SocketAccepter<SocketPayload>
		implements PayloadFactory<SocketPayload> {
	public PlainSocketAccepter(MultiStageServer<SocketPayload> server,
			InetAddress addr, int port, Stage<SocketPayload> startStage,
			boolean blocking) {
		super.init(server, addr, port, startStage, this, blocking);
	}

	public SocketPayload makePayload(SocketChannel sockChan) {
		return new SocketPayload(sockChan);
	}
}
