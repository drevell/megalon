package org.megalon.multistageserver;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.megalon.multistageserver.MultiStageServer.NextAction;
import org.megalon.multistageserver.MultiStageServer.NextAction.Action;
import org.megalon.multistageserver.MultiStageServer.Stage;

public class TestServer {
	Stage1 firstStage;
	Stage2 secondStage;
	Stage3 thirdStage;
	
	public TestServer() {
		firstStage = new Stage1();
		secondStage = new Stage2();
		thirdStage = new Stage3();
	}
	
	class Stage1 implements MultiStageServer.Stage<SocketPayload> {
		public NextAction<SocketPayload> runStage(SocketPayload work) {
			System.out.println("In stage 1");
			return new NextAction<SocketPayload>(Action.FORWARD, secondStage);
		}

		public int getNumConcurrent() {
			return 1;
		}

		public String getName() {
			return "Stage 1";
		}

		public int getBacklogSize() {
			return 5;
		}

		public void setServer(MultiStageServer<SocketPayload> server) {}
	}
	
	class Stage2 implements MultiStageServer.Stage<SocketPayload> {
		public NextAction<SocketPayload> runStage(SocketPayload work) 
		throws Exception {
			System.out.println("In stage 2");
			ByteBuffer buf = ByteBuffer.wrap("It's stage 2 yall\n".getBytes());
			System.out.println("work is " + work);
			System.out.println("work.sockChan is " + work.sockChan);
			work.sockChan.write(buf);
			return new NextAction<SocketPayload>(Action.FORWARD, thirdStage);
		}

		public int getNumConcurrent() {
			return 1;
		}

		public String getName() {
			return "Stage 2";
		}

		public int getBacklogSize() {
			return 5;
		}

		public void setServer(MultiStageServer<SocketPayload> server) {}
	}
	
	static class Stage3 implements MultiStageServer.Stage<SocketPayload> {
		public NextAction<SocketPayload> runStage(SocketPayload work) 
		throws Exception {
			System.out.println("In stage 3, throwing exception");
			throw new Exception("O...M....G!");
		}
		
		public int getNumConcurrent() {
			return 1;
		}

		public String getName() {
			return "Stage 3";
		}

		public int getBacklogSize() {
			return 5;
		}

		public void setServer(MultiStageServer<SocketPayload> server) {}
	}
	
	public static void main(String args[]) {
		new TestServer().run();
	}
	
	void run() {
		Set<Stage<SocketPayload>> stages = new HashSet<Stage<SocketPayload>>();
		stages.add(firstStage);
		stages.add(secondStage);
		stages.add(thirdStage);
		
		MultiStageServer<SocketPayload> server =  
			new MultiStageServer<SocketPayload>(stages);
		
		PlainSocketAccepter acc = new PlainSocketAccepter(server, null, 50000,
				firstStage, true);
		
		try {
			acc.runForever();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
