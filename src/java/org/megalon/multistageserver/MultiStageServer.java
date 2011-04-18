package org.megalon.multistageserver;

import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
public class MultiStageServer<T extends MultiStageServer.Payload> {
	protected Log logger = LogFactory.getLog(MultiStageServer.class);
	protected Map<Integer,StageDesc<T>> stages; 
	protected Map<Integer,ThreadPoolExecutor> execs;
	protected ThreadPoolExecutor finisherExec;
	boolean inited = false;
	
	/**
	 * Constructor that just wraps a call to init().
	 */
	public MultiStageServer(Map<Integer,StageDesc<T>> stages) {
		init(stages);
	}
	
	/**
	 * No-arg constructor. If you use this, you must explicitly initialize the
	 * server by calling init() before it will be usable.
	 */
	public MultiStageServer() { }
	
	/**
	 * Configure the server. This is required before the server can accept
	 * tasks via enqueue(). This function may be called for you by a 
	 * constructor.
	 * 
	 * @param stages a description of the server. The server is represented
	 * as a directed graph of Stage objects. Each stage has a unique integer ID
	 * and is described by a StageDesc object. So by passing a map of
	 * integer->StageDesc to this function, you completely define a server.
	 * @param finisherExec if payload objects have a non-null payload.finisher,
	 * then finisherExec specifies an executor that should run the finisher. For
	 * example, a socket server may want to set up an finisher executor that
	 * will send the server results over the socket and close it. If the caller
	 * passes null for the finisher, the server will set up a ThreadPoolExecutor
	 * internally with a small number of threads to run the payload finishers. 
	 */
	public void init(Map<Integer,StageDesc<T>> stages, ThreadPoolExecutor 
			finisherExec) {
		if(this.logger == null) {
			throw new AssertionError("Null logger... d'oh");
		}
		
		this.stages = stages;
		this.execs = setupExecutors(stages);
		if(finisherExec != null) {
			this.finisherExec = finisherExec;
		} else {
			this.finisherExec = new ThreadPoolExecutor(1, 3, 100, 
					TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		}
		inited = true;
	}
	
	/**
	 * A wrapper around init() that uses the default logger.
	 */
	public void init(Map<Integer,StageDesc<T>> stages) {
		init(stages, null);
	}
	
	/**
	 * This is the parent class for objects that are passed between the stages
	 * of the server. They contain request state.
	 * 
	 * There are two ways to run code after request handling completes. One way
	 * is to set a "finisher," which is an object implementing finish(payload),
	 * which will be run after the last server stage completes. The other way
	 * is to call waitFinished(), which will block the calling thread until
	 * the server completes and calls notifyAll().
	 */
	static public class Payload {
		private boolean isFinished = false;
		public SocketChannel sockChan;
		public Throwable throwable;
		@SuppressWarnings("rawtypes")
		public Finisher finisher;
		
		public Payload(SocketChannel sockChan) {
			this(sockChan, null);
		}
		
		public Payload(SocketChannel sockChan, 
				@SuppressWarnings("rawtypes") Finisher finisher) {
			this.sockChan = sockChan;
			this.finisher = finisher;
		}
		
		/**
		 * Block until all server stages have finished processing.
		 */
		synchronized public void waitFinished() {
			while(!isFinished) {
				try {
					wait();
				} catch (InterruptedException e) {}
			}
		}
		
		/**
		 * Called by a StageRunner when the final stage is done to indicate
		 * completion. 
		 * @param e any exception that may have occurred in the last stage
		 */
		synchronized void setFinished(Throwable e) {
			this.isFinished = true;
			this.throwable = e;
			notifyAll();
		}

		/** 
		 * Called by StageRunner to indicate error-free completion of server
		 * processing.
		 */
		void setFinished() {
			setFinished(null);
		}

	}

	public interface Finisher<T extends Payload> {
		/**
		 * Called when a server has finished processing a request.
		 * @param p the request payload, possibly containing server results
		 */
		void finish(T payload);
	}

	
	public boolean enqueue(T payload, int startStage) {
		if(!inited) {
			logger.warn("Enqueue attempt for un-inited server");
			return false;
		}
		StageRunner sr = new StageRunner(startStage, payload);
		ThreadPoolExecutor exec = execs.get(startStage);
		if(exec == null) {
			return false;
		} else {
			exec.execute(sr);
			return true;
		}
	}
	
	/**
	 * A simple struct that describes one of the stages inside a staged server.
	 */
	public static class StageDesc<T extends Payload> {
		protected int numConcurrent;
		protected Stage<T> stage;
		protected String name;
		public StageDesc(int numConcurrent, Stage<T> stage, String name) {
			this.numConcurrent = numConcurrent;
			this.stage = stage;
			this.name = name;
		}
		public int getNumConcurrent() {
			return numConcurrent;
		}
		public Stage<T> getStage() {
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
	public interface Stage<T> {
		/**
		 * Classes that implement this interface should implement this method
		 * to do whatever actual work takes place in that stage. For example,
		 * a stage might write data to disk or send a message to the client.
		 * 
		 * @return the integer ID of the next state to be entered for this
		 * request, or -1 to halt cleanly.
		 */
		public abstract int runStage(T payload) throws Exception;
	}
	
	protected Map<Integer,ThreadPoolExecutor> setupExecutors(
			Map<Integer,StageDesc<T>> stages) {
		// For each server stage, set up a thread pool and queue of pending work
		Map<Integer,ThreadPoolExecutor> stageExecutors = 
				new ConcurrentHashMap<Integer, ThreadPoolExecutor>();
		for(Map.Entry<Integer,StageDesc<T>> e : stages.entrySet()) {
			StageDesc<T> stageDesc = e.getValue();
			int stageIdNum = e.getKey();
			
			BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
			ThreadPoolExecutor exec = new ThreadPoolExecutor(1,	
					stageDesc.numConcurrent, 100, TimeUnit.MILLISECONDS, queue);
			stageExecutors.put(stageIdNum, exec);
		}
		return stageExecutors;
	}
	
	/**
	 * RunStage is a runnable that will be placed in an executor queue. When a 
	 * thread takes it off the queue and runs it, it will run the single stage
	 * that was provided to the constructor. When it has finished running that
	 * stage, it will know the ID of the stage to be executed next for that
	 * request. The request will be passed on to the queue for the next stage. 
	 */
	class StageRunner implements Runnable {
		protected int stageToRun;
		T payload;
		public StageRunner(int stageToRun, T payload) {
			this.stageToRun = stageToRun;
			this.payload = payload;
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
				return false;
			}
		}

		/**
		 * Once the ThreadPoolExecutor has an available thread, this will be 
		 * called. It will execute the stage and enqueue a work unit for the 
		 * next stage if another stage is indicated.
		 */
		public void run() {
			Stage<T> curStage = stages.get(stageToRun).getStage();
			Exception errException = null;
			try {
				stageToRun = curStage.runStage(payload);
				// TODO prevent reordering of memory ops between stages?

				if(stageToRun == -1) {
					logger.debug("Request stopping cleanly.");
				} else if(!enqueue()) {
					errException = new Exception(
							"enqueue failed, no such stage " + stageToRun);
				}
			} catch (Exception e) {
				e.printStackTrace();
				errException = new Exception("Exception in stage: " + 
						stages.get(stageToRun).getName(), e);
				stageToRun = -1;

			}

			if(errException != null) {
				logger.warn("Exception in StageRunner", errException);
			}
			
			// Trigger the two post-processing mechanisms, finishers and 
			// waiters, in that order.
			if(stageToRun == -1) {
				if(payload.finisher != null) {
					Runnable finishRunner = new Runnable() {
						@SuppressWarnings("unchecked")
						public void run() {
							payload.finisher.finish(payload);
						}
					};
					finisherExec.execute(finishRunner);
				}
				payload.setFinished(errException);
			}
		}
	}
}
