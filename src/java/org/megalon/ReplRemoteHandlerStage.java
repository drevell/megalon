package org.megalon;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.messages.MegalonMsg;
import org.megalon.messages.MsgAccept;
import org.megalon.messages.MsgAcceptResp;
import org.megalon.messages.MsgPrepare;
import org.megalon.messages.MsgPrepareResp;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.NextAction;
import org.megalon.multistageserver.MultiStageServer.NextAction.Action;

/**
 * This stage is used by the replication server to process incoming prepare and
 * accept requests from other replicas.
 */
public class ReplRemoteHandlerStage implements MultiStageServer.Stage<MReplPayload> {
	Log logger = LogFactory.getLog(ReplRemoteHandlerStage.class);
//	Stage<MPayload> nextStage;
	MultiStageServer<MReplPayload> server;
	WAL wal;
	
	public ReplRemoteHandlerStage(WAL wal) {
//		this.nextStage = nextStage;
		this.wal = wal;
	}
	
	public NextAction<MReplPayload> runStage(MReplPayload payload) throws Exception {
		logger.debug("ReplExecStage running");
		switch(payload.msgType) {
		case MegalonMsg.MSG_PREPARE:
			MsgPrepare prepMsg = (MsgPrepare)payload.req;
			WALEntry entry = null;
			try {
				logger.debug("To wal.prepareLocal()");
				entry = wal.prepareLocal(prepMsg.walIndex, prepMsg.n);
			} catch (IOException e) {
				logger.debug("IOException in WAL prepareLocal");
				entry = null;
			}
			payload.resp = new MsgPrepareResp(entry, false);
			break;
		case MegalonMsg.MSG_ACCEPT:
			MsgAccept accMsg = (MsgAccept)payload.req;
			boolean result;
			try {
				result = wal.acceptLocal(accMsg.walIndex, 
						accMsg.walEntry);
			} catch (IOException e) {
				result = false;
			}
			payload.resp = new MsgAcceptResp(result);
			break;
		default:
			logger.warn("Unrecognized msg type: " + payload.msgType);
			payload.resp = null;
			break;
		}
		return new NextAction<MReplPayload>(Action.FINISHED, null);
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

	public void setServer(MultiStageServer<MReplPayload> server) {
		this.server = server;
	}
}
