package org.megalon.multistageserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Set;

import org.megalon.multistageserver.MultiStageServer.NextAction.Action;

/**
 * A demo: a simple one-stage server that echos back its input. It unnecessarily
 * uses tricky features like Finisher and payload waiting for demo purposes.
 */

// TODO use SelectorStage here for demo purposes

public class EchoServer extends MultiStageServer<EchoServer.EchoPayload> 
implements SocketAccepter.PayloadFactory<EchoServer.EchoPayload>, 
MultiStageServer.Finisher<EchoServer.EchoPayload>
{
	static final public int BUFSIZE = 2048;
	static final public int ECHO_PORT = 7; // well-known echo port
	static final public int NUMTHREADS = 10;
	
	EchoStage echoStage = new EchoStage();
	
	public EchoServer() throws Exception {
		echoStage = new EchoStage();
		Set<Stage<EchoPayload>> stages = new HashSet<Stage<EchoPayload>>();
		stages.add(echoStage);
		super.init(stages);	
	}
	
	/**
	 * The server consists of a single stage that reads a some input and
	 * echoes it back. This stage forwards the request back to the same stage
	 * when it's done echoing one buffer's worth of input.
	 */
	class EchoStage implements Stage<EchoPayload> {
		public NextAction<EchoPayload> runStage(EchoPayload payload) 
		throws IOException {
			SocketChannel sockChan = payload.sockChan;
			if(payload.bb == null) {
				payload.bb = ByteBuffer.allocate(BUFSIZE);
			}
			if(sockChan.read(payload.bb) == -1) {
				return new NextAction<EchoPayload>(Action.FINISHED, null);
			}
			payload.bb.flip();
			sockChan.write(payload.bb);
			payload.bb.flip();
			payload.countEchoed++;
			return new NextAction<EchoPayload>(Action.FORWARD, echoStage);
		}

		public int getNumConcurrent() {
			return 5;
		}

		public String getName() {
			return "EchoStage";
		}

		public int getBacklogSize() {
			return 5;
		}

		public void setServer(MultiStageServer<EchoPayload> server) {}
	}
	
	class EchoPayload extends SocketPayload {
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
			sa.init(server, null, ECHO_PORT, server.echoStage, server, true);
			sa.runForever();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public EchoPayload makePayload(SocketChannel sockChan) {
		EchoPayload pl = new EchoPayload(sockChan);
		new Thread(new PayloadWaiter(pl)).start();
		return pl;
	}

	public void finish(EchoPayload payload) {
		System.out.println("Socket closed after echoing " + 
				payload.countEchoed + " things");
	}
}
