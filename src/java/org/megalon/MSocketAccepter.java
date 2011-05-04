//package org.megalon;
//
//import java.net.InetAddress;
//import java.nio.channels.SocketChannel;
//
//import org.megalon.multistageserver.MultiStageServer;
//import org.megalon.multistageserver.MultiStageServer.Stage;
//import org.megalon.multistageserver.SocketAccepter;
//
///**
// * This class extends the abstract SocketAccepter in a way that deals with
// * Megalon-specific payloads.
// */
//public class MSocketAccepter extends SocketAccepter<MPayload> implements 
//SocketAccepter.PayloadFactory<MPayload> {
//	
//	public MSocketAccepter(MultiStageServer<MPayload> server, InetAddress addr,
//			int port, Stage<MPayload> startStage) {
//		super.init(server, addr, port, startStage, this, false);
//	}
//
//	public MPayload makePayload(SocketChannel sockChan) {
//		return new MPayload(sockChan);
//	}
//
//	public void finished(MPayload payload) {
//		close(payload.sockChan);
//	}
//}
