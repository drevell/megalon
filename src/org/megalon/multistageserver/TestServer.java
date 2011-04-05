package org.megalon.multistageserver;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.megalon.multistageserver.MultiStageServer.ReqMetaData;
import org.megalon.multistageserver.MultiStageServer.StageDesc;

public class TestServer {
	static final int FIRST_STAGE = 0;
	static final int SECOND_STAGE = 1;
	static final int THIRD_STAGE = 2;
	
	public static void main(String args[]) {
		if(args.length > 0 && args[0].equals("background")) {
			run(false);
		} else {
			run(true);
		}
	}
	
	static class Stage1 implements MultiStageServer.Stage {
		public int runStage(ReqMetaData work) {
			System.out.println("In stage 1");
			return SECOND_STAGE;
		}
	}
	
	static class Stage2 implements MultiStageServer.Stage {
		public int runStage(ReqMetaData work) throws Exception {
			System.out.println("In stage 2");
			ByteBuffer buf = ByteBuffer.wrap("It's stage 2 yall\n".getBytes());
			work.sockChan.write(buf);
			return THIRD_STAGE;
		}
	}
	
	static class Stage3 implements MultiStageServer.Stage {
		public int runStage(ReqMetaData work) throws Exception {
			System.out.println("In stage 3, throwing exception");
			throw new Exception("O...M....G!");
//			return -1;
		}		
	}
	
	static void run(boolean foreground) {
		Map<Integer, StageDesc> stages = new HashMap<Integer,StageDesc>();
		stages.put(FIRST_STAGE, new MultiStageServer.StageDesc(5, new Stage1(), "stage1"));
		stages.put(SECOND_STAGE, new MultiStageServer.StageDesc(5, new Stage2(), "stage2"));
		stages.put(THIRD_STAGE, new MultiStageServer.StageDesc(5, new Stage3(), "stage3"));
		MultiStageServer server = new MultiStageServer();
		
		try {
			if(foreground) {
				System.out.println("Running in the foreground");
				server.runForever(null, 50000, stages, FIRST_STAGE);
			} else {
				System.out.println("Running in the background");
				server.runBackground(null, 50000, stages, FIRST_STAGE);
				System.out.println("Server started, sleeping");
				Thread.sleep(2000);
				System.out.println("Stopping server...");
				server.stopBackground();
				System.out.println("Done");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
