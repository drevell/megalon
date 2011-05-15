package org.megalon;

import java.nio.channels.SocketChannel;

import org.megalon.messages.MegalonMsg;
import org.megalon.multistageserver.Payload;
import org.megalon.multistageserver.SocketAccepter.PayloadFactory;
import org.megalon.multistageserver.SocketPayload;

public class MSocketPayload extends SocketPayload {
	public long rpcSerial; 
	
	// After the core server has finished processing, this will hold its response.
	protected MegalonMsg resp; 

	public MSocketPayload(SocketChannel sockChan) {
		super(sockChan);
	}
	
	public MSocketPayload(SocketChannel sockChan, Payload wrapPayload,
			long rpcSerial, byte reqType) {
		super(sockChan, wrapPayload);
		this.rpcSerial = rpcSerial;
	}
	
	static public class Factory implements PayloadFactory<MSocketPayload> {
		public MSocketPayload makePayload(SocketChannel sockChan) {
			return new MSocketPayload(sockChan);
		}
	}
	
	public void setResponse(MegalonMsg resp) {
		this.resp = resp;
	}
	
//	public MSocketPayload(byte msgType, Payload outerPayload) {
//		super(outerPayload);
//		this.msgType = msgType;
//	}
}
