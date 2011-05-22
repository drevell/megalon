package org.megalon.multistageserver;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.multistageserver.MultiStageServer.NextAction.Action;

/**
 * This class offers a concise way to express a server as a set of "stages."
 * Each stage consists of a finite thread pool and a Stage object. This design
 * allows fine control of how many threads are concurrently running any
 * particular piece of code, and how many global threads there are.
 * 
 * The main benefit of this design is that concurrency can be fixed at the level
 * that gives the highest throughput, and adding further load will result in
 * requests being gracefully denied instead of causing the server to thrash.
 * 
 * This design uses "inversion of control" in the sense that the caller calls
 * runForever() which never returns. Of course, the Stage objects specified by
 * the caller will be used to handle incoming requests.
 * 
 * This design is similar to a (massively simplified) version of Matt Welsh's
 * SEDA architecture. It also looks a lot like the Apache Cassandra
 * interpretation of SEDA.
 */
public class MultiStageServer<T extends Payload> {
	protected Log logger = LogFactory.getLog(MultiStageServer.class);
	protected Set<Stage<T>> stages;
	protected Map<Stage<T>, ThreadPoolExecutor> execs;
	protected ThreadPoolExecutor finisherExec;
	protected ThreadFactory daemonThreadFactory;
	boolean inited = false;
	String name;

	// TODO consider scrapping NextAction and make stages explicitly enqueue
	static public class NextAction<T extends Payload> {
		public enum Action {
			FORWARD, IGNORE, FINISHED
		};

		Action action;
		Stage<T> nextStage;

		public NextAction(Action action, Stage<T> nextStage) {
			if (action == Action.FORWARD && nextStage == null) {
				throw new AssertionError("Can't forward request to null stage");
			}
			this.action = action;
			this.nextStage = nextStage;
		}
	}

	/**
	 * Constructor that just wraps a call to init().
	 */
	public MultiStageServer(String name, Set<Stage<T>> stages) throws Exception {
		init(name, stages);
	}

	/**
	 * No-arg constructor. If you use this, you must explicitly initialize the
	 * server by calling init() before it will be usable.
	 */
	public MultiStageServer() {
	}

	/**
	 * Configure the server. This is required before the server can accept tasks
	 * via enqueue(). This function may be called for you by a constructor.
	 * 
	 * @param stages
	 *            a description of the server. The server is represented as a
	 *            directed graph of Stage objects. Each stage has a unique
	 *            integer ID and is described by a StageDesc object. So by
	 *            passing a map of integer->StageDesc to this function, you
	 *            completely define a server.
	 * @param finisherExec
	 *            if payload objects have a non-null payload.finisher, then
	 *            finisherExec specifies an executor that should run the
	 *            finisher. For example, a socket server may want to set up an
	 *            finisher executor that will send the server results over the
	 *            socket and close it. If the caller passes null for the
	 *            finisher, the server will set up a ThreadPoolExecutor
	 *            internally with a small number of threads to run the payload
	 *            finishers.
	 */
	public void init(String name, Set<Stage<T>> stages, ThreadPoolExecutor finisherExec)
			throws Exception {
		this.name = name;
		// Make a ThreadFactory that produces daemon threads
		daemonThreadFactory = new ThreadFactory() {
			public Thread newThread(Runnable r) {
				Thread t = Executors.defaultThreadFactory().newThread(r);
				t.setDaemon(true);
				return t;
			}
		};

		this.stages = stages;
		for (Stage<T> stage : stages) {
			stage.setServer(this);
		}

		this.execs = setupExecutors(stages);
		if (finisherExec != null) {
			this.finisherExec = finisherExec;
		} else {
			this.finisherExec = new ThreadPoolExecutor(1, 3, 100,
					TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
					daemonThreadFactory);
		}
		inited = true;
	}

	/**
	 * A wrapper around init() that uses the default finisher executor pool. If
	 * you don't know what a finisher executor pool is, this is the right init()
	 * function for you.
	 */
	public void init(String name, Set<Stage<T>> stages) throws Exception {
		init(name, stages, null);
	}

	public interface Finisher<T extends Payload> {
		/**
		 * Called when a server has finished processing a request.
		 * 
		 * @param p
		 *            the request payload, possibly containing server results
		 */
		void finish(T payload);
	}

	public boolean enqueue(T payload, Stage<T> startStage) {
		return enqueue(payload, startStage, null);
	}

	public boolean enqueue(T payload, Stage<T> startStage, Finisher finisher) {
		if (!inited) {
			logger.warn("Enqueue attempt for un-inited server");
			return false;
		}
		StageRunner sr = new StageRunner(startStage, payload);
		ThreadPoolExecutor exec = execs.get(startStage);
		if (exec == null) {
			logger.warn("Enqueue into stage with null executor, server " + name,
					new Exception()); // Exception will print stack trace
			return false;
		} else {
			// logger.debug("Before enqueue: " + Util.executorStatus(exec));
			payload.finisher = finisher;
			payload.barrier = true; // memory fence
			exec.execute(sr);
			logger.debug("Request entered server at stage: " + startStage);
			return true;
		}
	}

	/**
	 * All server stages should implement this interface. For instance,
	 * AuthenticateStage, HandshakeStage, FlushStage, etc.
	 */
	public interface Stage<T extends Payload> {
		/**
		 * Classes that implement this interface should implement this method to
		 * do whatever actual work takes place in that stage. For example, a
		 * stage might write data to disk or send a message to the client.
		 */
		public abstract NextAction<T> runStage(T payload) throws Exception;

		public abstract int getNumConcurrent();

		public abstract String getName();

		public abstract int getBacklogSize();

		public abstract void setServer(MultiStageServer<T> server);
	}

	/**
	 * For each server stage, set up a thread pool and queue of pending work.
	 */
	protected Map<Stage<T>, ThreadPoolExecutor> setupExecutors(
			Set<Stage<T>> stages) throws Exception {
		Map<Stage<T>, ThreadPoolExecutor> stageExecutors = new ConcurrentHashMap<Stage<T>, ThreadPoolExecutor>();
		for (Stage<T> stage : stages) {
			try {
				BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(
						stage.getBacklogSize());
				int concurrent = stage.getNumConcurrent();
				ThreadPoolExecutor exec = new ThreadPoolExecutor(concurrent,
						concurrent, 100, TimeUnit.MILLISECONDS, queue,
						daemonThreadFactory);
				stageExecutors.put(stage, exec);
			} catch (Throwable e) {
				String errMsg = "Error initializing stage";
				logger.error(errMsg, e);
				throw new Exception(errMsg, e);
			}
		}
		return stageExecutors;
	}

	/**
	 * Trigger the two post-processing mechanisms, finishers and waiters,
	 * concurrently.
	 */
	public void finishPayload(T payload, Throwable excOrNull) {
		final T finPayload = payload;
		if (payload.finisher != null) {
			Runnable finishRunner = new Runnable() {
				@SuppressWarnings("unchecked")
				public void run() {
					finPayload.finisher.finish(finPayload);
				}
			};
			payload.barrier = true; // memory barrier
			// logger.debug("Enqueueing finisher runnable");
			finisherExec.execute(finishRunner);
		}
		payload.setFinished(excOrNull);
	}

	public void finishPayload(T payload) {
		finishPayload(payload, null);
	}

	/**
	 * RunStage is a runnable that will be placed in an executor queue. When a
	 * thread takes it off the queue and runs it, it will run the single stage
	 * that was provided to the constructor. When it has finished running that
	 * stage, it will know the ID of the stage to be executed next for that
	 * request. The request will be passed on to the queue for the next stage.
	 */
	class StageRunner implements Runnable {
		protected Stage<T> stageToRun;
		T payload;

		public StageRunner(Stage<T> stageToRun, T payload) {
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
			if (nextStageExecutor != null) {
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
			// logger.debug("StageRunner going to run: " +
			// stageToRun.getName());
			Exception errException = null;
			NextAction<T> nextAction = null;
			try {
				payload.barrier = true; // memory barrier
				nextAction = stageToRun.runStage(payload);
				// logger.debug("StageRunner stage finished: "
				//		+ stageToRun.getName());
			} catch (Throwable e) {
				errException = new Exception("Exception in server: " + name + 
						", stage: " + stageToRun.getName(), e);
				nextAction = new NextAction<T>(Action.FINISHED, null);
			}

			if (nextAction.action == Action.FORWARD
					&& nextAction.nextStage == null) {
				logger.warn("nextAction was FORWARD, but nextStage was null!");
				nextAction.action = Action.FINISHED;
			}

			switch (nextAction.action) {
			case FINISHED:
				if (errException != null) {
					logger.warn("Exception in StageRunner", errException);
				} else {
					// logger.debug("Request stopping cleanly with FINISHED");
				}
				finishPayload(payload); // Run finishers and waiters on payload
				break;
			case FORWARD:
				// logger.debug("Stage \"" + stageToRun.getName() + "\" done, "
				// +
				// "forwarding to stage \"" + nextAction.nextStage.getName() +
				// "\"");
				stageToRun = nextAction.nextStage;
				if (!enqueue()) {
					logger.warn("Nonexistent stage (is it the wrong server?): "
							+ stageToRun.getName());
				}
				break;
			case IGNORE:
				// logger.debug("Stage \"" + stageToRun +
				// "\" OK, not forwarding "+
				// "due to action IGNORE");
				break;
			default:
				throw new AssertionError("The deuce? Unknown action");
			}
		}
	}
}
