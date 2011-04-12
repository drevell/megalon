package org.megalon;

import java.net.InetAddress;
import java.nio.channels.SocketChannel;

import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.SocketAccepter;

/**
 * This class extends the abstract SocketAccepter in a way that deals with
 * Megalon-specific payloads.
 */
public class MSocketAccepter extends SocketAccepter<MPayload> implements 
SocketAccepter.PayloadFactory<MPayload> {
	
	public MSocketAccepter(MultiStageServer<MPayload> server, InetAddress addr,
			int port, int startStage) {
		super.init(server, addr, port, startStage, this);
	}

	@Override
	public MPayload makePayload(SocketChannel sockChan) {
		return new MPayload(sockChan);
	}

	@Override
	public void finished(MPayload payload) {
		close(payload.sockChan);
	}
	
	
}
