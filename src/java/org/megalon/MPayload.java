package org.megalon;

import java.nio.channels.SocketChannel;

import org.megalon.multistageserver.MultiStageServer;

public class MPayload extends MultiStageServer.Payload {
	public MPayload(SocketChannel sockChan) {
		super(sockChan);
	}
}
