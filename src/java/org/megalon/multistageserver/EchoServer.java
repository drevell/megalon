package org.megalon.multistageserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * A demo: a simple one-stage server that echos back its input. It unnecessarily
 * uses tricky features like Finisher and payload waiting for demo purposes.
 */
public class EchoServer extends MultiStageServer<EchoServer.EchoPayload> 
implements SocketAccepter.PayloadFactory<EchoServer.EchoPayload>, 
MultiStageServer.Finisher<EchoServer.EchoPayload>
{
	static final public int ECHO_STAGE_ID = 1;
	static final public int BUFSIZE = 2048;
	static final public int ECHO_PORT = 7; // well-known echo port
	static final public int NUMTHREADS = 10;

	public EchoServer() {
		Map<Integer,StageDesc<EchoPayload>> stages = 
			new HashMap<Integer,StageDesc<EchoPayload>>();
		stages.put(ECHO_STAGE_ID, new StageDesc<EchoPayload>(NUMTHREADS, 
				new EchoStage(), "my echo stage"));
		super.init(stages);	
	}
	
	static class EchoStage implements Stage<EchoPayload> {
		public int runStage(EchoPayload payload) throws IOException {
			SocketChannel sockChan = payload.sockChan;
			if(payload.bb == null) {
				payload.bb = ByteBuffer.allocate(BUFSIZE);
			}
			if(sockChan.read(payload.bb) == -1) {
				return -1;
			}
			payload.bb.flip();
			sockChan.write(payload.bb);
			payload.bb.flip();
			payload.countEchoed++;
			return ECHO_STAGE_ID;
		}
	}
	
	class EchoPayload extends MultiStageServer.Payload {
		int countEchoed;
		ByteBuffer bb;
		
		public EchoPayload(SocketChannel sockChan) {
			super(sockChan);
			finisher = EchoServer.this;
			countEchoed = 0;
		}
	}
	
	class PayloadWaiter implements Runnable {
		EchoPayload payload;
		
		public PayloadWaiter(EchoPayload payload) {
			this.payload = payload;
		}
		public void run() {
			payload.waitFinished();
			System.out.println("PayloadWaiter running after request complete.");
		}
	}
	
	public static void main(String[] args) {
		
		try {
			EchoServer server = new EchoServer();
			SocketAccepter<EchoPayload> sa = new SocketAccepter<EchoPayload>();
			sa.init(server, null, ECHO_PORT, ECHO_STAGE_ID, server);
			sa.runForever();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public EchoPayload makePayload(SocketChannel sockChan) {
		EchoPayload pl = new EchoPayload(sockChan);
		new Thread(new PayloadWaiter(pl)).start();
		return pl;
	}

	@Override
	public void finish(EchoPayload payload) {
		System.out.println("Socket closed after echoing " + 
				payload.countEchoed + " things");
	}
}
