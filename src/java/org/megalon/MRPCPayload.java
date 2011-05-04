//package org.megalon;
//
//import java.nio.channels.SocketChannel;
//
//import org.megalon.multistageserver.Payload;
//import org.megalon.multistageserver.SocketPayload;
//
//public class MRPCPayload extends SocketPayload {
//
//	public MRPCPayload(SocketChannel sockChan) {
//		super(sockChan);
//	}
//
//	public MRPCPayload(SocketChannel sockChan, Payload outerPayload) {
//		super(sockChan, outerPayload);
//	}
//	
//	/**
//	 * Checks whether the incoming buffers hold a complete RPC message. That is,
//	 * check that the length prefix is present, and that many bytes come after
//	 * it.
//	 */
//	public boolean keepSelecting(boolean timedOut) {
//		return false; // TODO keep selecting on partial messages
//	}
//}
