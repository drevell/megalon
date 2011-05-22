package org.megalon;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.WALEntry.Status;
import org.megalon.messages.MsgAccept;
import org.megalon.messages.MsgAcceptResp;
import org.megalon.messages.MsgChosen;
import org.megalon.messages.MsgPrepare;
import org.megalon.messages.MsgPrepareResp;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.NextAction;
import org.megalon.multistageserver.MultiStageServer.NextAction.Action;

/**
 * This stage is used by the replication server to process incoming prepare and
 * accept requests from other replicas.
 */
public class ReplRemoteHandlerStage implements MultiStageServer.Stage<MPayload> {
	Log logger = LogFactory.getLog(ReplRemoteHandlerStage.class);
	MultiStageServer<MPayload> server;
	WAL wal;
	ReplServer replServer;
	
	public ReplRemoteHandlerStage(ReplServer replServer, WAL wal) {
		this.replServer = replServer;
		this.wal = wal;
	}
	
	public NextAction<MPayload> runStage(MPayload payload) throws Exception {
		// logger.debug("ReplExecStage running");
		switch(payload.msgType) {
		case MsgPrepare.MSG_ID:
			MsgPrepare prepMsg = (MsgPrepare)payload.req;
			WALEntry entry = null;
			try {
				logger.debug("To wal.prepareLocal()");
				entry = wal.prepareLocal(prepMsg.entityGroup, prepMsg.walIndex, prepMsg.n);
			} catch (IOException e) {
				logger.debug("IOException in WAL prepareLocal");
				entry = null;
			}
			payload.resp = new MsgPrepareResp(entry, false);
			break;
		case MsgAccept.MSG_ID:
			MsgAccept accMsg = (MsgAccept)payload.req;
			boolean result;
			try {
				logger.debug("Accept for walEntry: " + accMsg.walEntry);
				result = wal.acceptLocal(accMsg.entityGroup, accMsg.walIndex, 
						accMsg.walEntry);
				logger.debug("Repl core: acceptLocal returned " + result);
			} catch (IOException e) {
				logger.warn("Repl core: acceptLocal exception", e);
				result = false;
			}
			payload.resp = new MsgAcceptResp(result);
			break;
		case MsgChosen.MSG_ID:
			// We must have sent an accept-ack for a value which turned out to
			// be the Paxos chosen value, which caused the proposer to send us
			// a Chosen message to inform us. Since the value in our WAL is the
			// Paxos chosen value, we will write its changes to the main 
			// database.
			MsgChosen chosenMsg = (MsgChosen)payload.req;
			logger.debug("ReplServer marking walIndex chosen: " + chosenMsg.walIndex);
			WALEntry walEntry = wal.changeWalStatus(chosenMsg.entityGroup, 
					chosenMsg.walIndex, Status.CHOSEN);
			logger.debug("Applying changes: " + walEntry);
			if(ReplServer.applyChanges(replServer.megalon, walEntry)) {
				wal.changeWalStatus(chosenMsg.entityGroup, chosenMsg.walIndex, 
						Status.FLUSHED);
			}
			break;
		default:
			logger.warn("Unrecognized msg type: " + payload.msgType);
			payload.resp = null;
			break;
		}
		return new NextAction<MPayload>(Action.FINISHED, null);
	}

	public int getNumConcurrent() {
		return 10; // TODO configurable
	}

	public String getName() {
		return this.getClass().getName();  // TODO configurable
	}

	public int getBacklogSize() {
		return 50; // TODO configurable
	}

	public void setServer(MultiStageServer<MPayload> server) {
		this.server = server;
	}
}
