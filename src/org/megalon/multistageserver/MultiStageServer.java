package org.megalon.multistageserver;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * This class offers a concise way to express a server as a set of "stages."
 * Each stage consists of a finite thread pool and a Stage object. This
 * design allows fine control of how many threads are concurrently running
 * any particular piece of code, and how many global threads there are.
 * 
 * The main benefit of this design is that concurrency can be fixed at the
 * level that gives the highest throughput, and adding further load will
 * result in requests being gracefully denied instead of causing the server
 * to thrash.
 * 
 * This design uses "inversion of control" in the sense that the caller
 * calls runForever() which never returns. Of course, the Stage objects
 * specified by the caller will be used to handle incoming requests.
 * 
 * This design is similar to a (massively simplified) version of Matt Welsh's
 * SEDA architecture. It also looks a lot like the Apache Cassandra 
 * interpretation of SEDA.
 */
public class MultiStageServer {
	protected Logger logger;
	protected Thread daemonThread = null;
	protected ServerSocketChannel servChan; 
	protected boolean intentionalStop;
	/**
	 * Construct using the default logger.
	 */
	public MultiStageServer() {
		this(null);
	}
	
	/**
	 * Construct using a specified logger instead of the default.
	 */
	public MultiStageServer(Logger logger) {
		if(logger == null) {
			this.logger = Logger.getLogger(MultiStageServer.class);
		} else {
			this.logger = logger;
		}
		if(this.logger == null) {
			throw new AssertionError("doh");
		}
	}
	
	/**
	 * A simple struct that describes one of the stages inside a staged server.
	 */
	public static class StageDesc {
		protected int numConcurrent;
		protected Stage stage;
		protected String name;
		public StageDesc(int numConcurrent, Stage stage, String name) {
			this.numConcurrent = numConcurrent;
			this.stage = stage;
			this.name = name;
		}
		public int getNumConcurrent() {
			return numConcurrent;
		}
		public Stage getStage() {
			return stage;
		}
		public String getName() {
			return name;
		}
	}

	/**
	 * All server stages should implement this interface.
	 * For instance, AuthenticateStage, HandshakeStage, FlushStage, etc.
	 */
	public interface Stage {
		/**
		 * Classes that implement this interface should implement this method
		 * to do whatever actual work takes place in that stage. For example,
		 * a stage might write data to disk or send a message to the client.
		 * 
		 * @return the integer ID of the next state to be entered for this
		 * request.
		 */
		public abstract int runStage(ReqMetaData work) throws Exception;
	}
	
	/**
	 * Represents the saved state of a client or request as it moves through 
	 * the server stages.
	 */
	public static class ReqMetaData {
		protected InetSocketAddress remoteAddr;
		protected SocketChannel sockChan;
		protected Object payload;
		public ReqMetaData(InetSocketAddress remoteAddr, SocketChannel sockChan) {
			this.remoteAddr = remoteAddr;
			this.sockChan = sockChan;
		}
		public InetSocketAddress getRemoteAddr() {
			return remoteAddr;
		}
		public SocketChannel getSockChan() {
			return sockChan;
		}
		public Object getPayload() {
			return payload;
		}
		public void setPayload(Object payload) {
			this.payload = payload;
		}
	}
	
	protected Map<Integer,ThreadPoolExecutor> setupExecutors(Map<Integer,StageDesc> stages) {
		// For each server stage, set up a thread pool and queue of pending work
		Map<Integer,ThreadPoolExecutor> stageExecutors = 
				new ConcurrentHashMap<Integer, ThreadPoolExecutor>();
		for(Map.Entry<Integer,StageDesc> e : stages.entrySet()) {
			StageDesc stageDesc = e.getValue();
			int stageIdNum = e.getKey();
			
			BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
			ThreadPoolExecutor exec = new ThreadPoolExecutor(1,	
					stageDesc.numConcurrent, 100, TimeUnit.MILLISECONDS, queue);
			stageExecutors.put(stageIdNum, exec);
		}
		return stageExecutors;
	}
	
	/**
	 * Start serving clients. Won't return unless there's an exception when 
	 * listening or accepting connections.
	 * @param addr the IP address to listen to
	 * @param port the TCP port to listen to
	 * @param stages A mapping from int->StageDesc. This associates each stage 
	 * with a unique integer ID. Mapping to a value of null is interpreted as
	 * an "end state," causing the connection to be closed.
	 * @param startStage The ID of the stage that should be entered for new clients.
	 */
	public void runForever(InetAddress addr, int port, 
			Map<Integer,StageDesc> stages, int startStage)throws IOException {
		Map<Integer,ThreadPoolExecutor> stageExecutors = setupExecutors(stages);
		
		// Bind to the desired IP/port
		servChan = ServerSocketChannel.open();
		servChan.socket().bind(new InetSocketAddress(addr, port));
		acceptLoop(servChan, startStage, stageExecutors, stages);
	}
		
	protected void acceptLoop(ServerSocketChannel servChan, int startStage, 
			Map<Integer,ThreadPoolExecutor> executors,
			Map<Integer,StageDesc> stages) throws IOException {
		// Loop forever, accepting and running the first stage
		while(true) {
			SocketChannel acceptedChan = servChan.accept();
			InetSocketAddress remoteAddr = (InetSocketAddress)
			   acceptedChan.socket().getRemoteSocketAddress();
			ReqMetaData work = new ReqMetaData(remoteAddr, acceptedChan);
			StageRunner firstStage = new StageRunner(work, startStage, stages, 
					executors);
			if(firstStage.enqueue() == false) {
				String errMsg = "The start stage (id " + startStage
						+ ") wasn't defined";
				logger.warn(errMsg);
				throw new AssertionError(errMsg);
			}
		}
	}
	
	/**
	 * Starts a new thread that will accept connections. This function will return
	 * as soon as the socket bind() completes sucessfully.
	 * @param addr
	 * @param port
	 * @param stages
	 * @param startStage
	 * @return
	 */
	synchronized public boolean runBackground(InetAddress addr, int port, 
			Map<Integer,StageDesc> stages, int startStage) throws IOException {
		class DaemonRunner implements Runnable {
			ServerSocketChannel servChan;
			int startStage; 
			Map<Integer,ThreadPoolExecutor> executors;
			Map<Integer,StageDesc> stages;
			
			public DaemonRunner(ServerSocketChannel servChan, int startStage, 
					Map<Integer,ThreadPoolExecutor> executors,
					Map<Integer,StageDesc> stages) {
				this.servChan = servChan;
				this.startStage = startStage;
				this.executors = executors;
				this.stages = stages;
			}
			public void run() {
				try {
					acceptLoop(servChan, startStage, executors, stages);
				} catch (IOException e) {
					if(!intentionalStop) {
						logger.warn("Exception in listening socket", e);
					} else {
						logger.trace("Intentional background daemon stop");
					}
				}
			}
		}
		
		Map<Integer,ThreadPoolExecutor> stageExecutors = setupExecutors(stages);
		
		// Bind to the desired IP/port
		servChan = ServerSocketChannel.open();
		servChan.socket().bind(new InetSocketAddress(addr, port));
		
		if(daemonThread != null) {
			logger.warn("Tried to runBackground but server was already running");
			return false;
		}
		intentionalStop = false;
		daemonThread = new Thread(new DaemonRunner(servChan, startStage, 
				stageExecutors, stages));
		daemonThread.start();
		return true;
	}
	
	synchronized public void stopBackground() {
		intentionalStop = true;
		try {
			logger.debug("Stopping backgrounded server");
			System.out.println("1");
			servChan.close();
			logger.trace("Listening channel closed, waiting for thread");
			System.out.println("2");
			daemonThread.join();
			logger.trace("Listening channel closed");
			System.out.println("3");
			daemonThread = null;
		} catch (Exception e) {
			System.out.println("4");
			e.printStackTrace();
			logger.warn("Exception in stopBackground when closing listen socket", e);
		}
	}
		
	
	/**
	 * RunStage is a runnable that will be placed in an executor queue. When a 
	 * thread takes it off the queue and runs it, it will run the single stage
	 * that was provided to the constructor. When it has finished running that
	 * stage, it will know the ID of the stage to be executed next for that
	 * request. The request will be passed on to the queue for the next stage. 
	 */
	class StageRunner implements Runnable {
		protected Map<Integer,StageDesc> stageDescs;
		protected ReqMetaData work;
		protected Map<Integer,ThreadPoolExecutor> execs;
		protected int stageToRun;
		public StageRunner(ReqMetaData work, int stageToRun, Map<Integer,StageDesc> stageDescs, Map<Integer,ThreadPoolExecutor> execs) {
			this.work = work;
			this.stageToRun = stageToRun;
			this.stageDescs = stageDescs;
			this.execs = execs;
		}
		
		/**
		 * Add this object to the executor queue for the stage that it should
		 * run, which will make it run as soon as a thread is available. We 
		 * already know what stage ID to use from the constructor.
		 */
		public boolean enqueue() {
			ThreadPoolExecutor nextStageExecutor = execs.get(stageToRun);
			if(nextStageExecutor != null) {
				nextStageExecutor.execute(this);
				return true;
			} else {
				close();
				return false;
			}
		}
		
		protected void close() {
			try {
				work.sockChan.close();
			} catch (IOException e) {}
		}
		
		/**
		 * Once the ThreadPoolExecutor has an available thread, this will be 
		 * called. It will execute the stage and enqueue a work unit for the 
		 * next stage if another stage is indicated.
		 */
		public void run() {
			Stage curStage = stageDescs.get(stageToRun).getStage();
			try {
				int nextStageId = curStage.runStage(work);
				StageRunner nextRunStage = new StageRunner(work, nextStageId, stageDescs, execs);
				nextRunStage.enqueue();
			} catch (Exception e) {
				e.printStackTrace();
				logger.warn("Closing socket due to exception in stage: " + 
						stageDescs.get(stageToRun).getName());
				close();
			}
		}
	}
}
