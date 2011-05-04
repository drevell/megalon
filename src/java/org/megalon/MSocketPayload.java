package org.megalon;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.megalon.messages.MegalonMsg;
import org.megalon.multistageserver.Payload;
import org.megalon.multistageserver.SocketAccepter.PayloadFactory;
import org.megalon.multistageserver.SocketPayload;

public class MSocketPayload extends SocketPayload {
	public MegalonMsg resp;
	public int reqType;
	public long rpcSerial; 
	
	public MSocketPayload(SocketChannel sockChan) {
		super(sockChan);
	}
	
	public MSocketPayload(SocketChannel sockChan, Payload wrapPayload,
			long rpcSerial) {
		super(sockChan, wrapPayload);
		this.rpcSerial = rpcSerial;
	}
	
	public void setReqType(int reqType) {
		this.reqType = reqType;
	}
	
	static public class Factory implements PayloadFactory<MSocketPayload> {
		public MSocketPayload makePayload(SocketChannel sockChan) {
			return new MSocketPayload(sockChan);
		}
	}}
