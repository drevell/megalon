package org.megalon.multistageserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * A demo: a simple one-stage server that echos back its input.
 */
public class EchoServer extends MultiStageServer {
	static final public int ECHO_STAGE_ID = 1;
	static final public int BUFSIZE = 2048;
	static final public int ECHO_PORT = 7; // well-known echo port
	static final public int NUMTHREADS = 10;
	
	static class EchoStage implements Stage {
		public int runStage(ReqMetaData work) throws IOException {
			SocketChannel sockChan = work.getSockChan();
			ByteBuffer bb = (ByteBuffer)work.getPayload();
			if(bb == null) {
				bb = ByteBuffer.allocate(BUFSIZE);
				work.setPayload(bb);
			}
			if(sockChan.read(bb) == -1) {
				return -1;
			}
			bb.flip();
			sockChan.write(bb);
			bb.flip();
			return ECHO_STAGE_ID;
		}
	}
	
	public static void main(String[] args) {
		Map<Integer,StageDesc> stages = new HashMap<Integer,StageDesc>();
		stages.put(ECHO_STAGE_ID, new StageDesc(NUMTHREADS, new EchoStage(), 
				"my echo stage"));
		try {
			new EchoServer().runForever(null, ECHO_PORT, stages, ECHO_STAGE_ID);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
