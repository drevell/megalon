package org.megalon;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.megalon.avro.AvroAccept;
import org.megalon.avro.AvroAcceptResponse;
import org.megalon.avro.AvroChosen;
import org.megalon.avro.AvroPrepare;
import org.megalon.avro.AvroPrepareResponse;
import org.megalon.messages.MegalonMsg;
import org.megalon.messages.MsgAccept;
import org.megalon.messages.MsgAcceptResp;
import org.megalon.messages.MsgChosen;
import org.megalon.messages.MsgPrepare;
import org.megalon.messages.MsgPrepareResp;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.Stage;
import org.megalon.multistageserver.SelectorStage;
import org.megalon.multistageserver.SocketAccepter;
import org.megalon.multistageserver.SocketPayload;

/**
 * This class implements the Replication Server component of megalon. It
 * does the same things as the Replication Server in Megastore (Paxos accepter,
 * apply changes to the WAL & main DB).
 */
public class ReplServer {
	static Log logger = LogFactory.getLog(ReplServer.class);
	Megalon megalon;
	InetSocketAddress proxyTo;
	WAL wal;
	MultiStageServer<MPayload> coreServer;
	final SocketAccepter<MSocketPayload> socketAccepter = 
		new SocketAccepter<MSocketPayload>();
	MultiStageServer<MSocketPayload> socketServer;
	boolean ready = false;
	Stage<MPayload> execStage;
	SelectorStage<MSocketPayload> selectorStage;
	AvroRpcDecode avroDecodeStage; 
	AvroRpcEncode avroEncodeStage; 
	Client paxos;
	
	Map<CmpBytes,HTable> cachedHTables = new HashMap<CmpBytes,HTable>();
	
	public ReplServer(Megalon megalon) throws IOException {
		this.megalon = megalon;
	}
	
	public void init() throws Exception {
		logger.debug("Replication server init'ing");
		wal = new WAL(megalon);

		// The coreServer contains the stages that are the same regardless of 
		// whether the request arrived by socket or by function call.
		Set<Stage<MPayload>> coreStages = new HashSet<Stage<MPayload>>();
		execStage = new ReplRemoteHandlerStage(this, wal);
		coreStages.add(execStage);
		coreServer = new MultiStageServer<MPayload>("replCore", coreStages);
		
		// The socketServer contains the stages that only run for socket
		// connections. The socketServer hands off requests to coreServer to
		// do the actual database operations.
		Set<Stage<MSocketPayload>> socketSvrStages = 
			new HashSet<Stage<MSocketPayload>>();
		avroDecodeStage = new AvroRpcDecode();
		selectorStage = new SelectorStage<MSocketPayload>(avroDecodeStage, 
				"replSelectorStage", 1, 50);
		
		// Set up the mapping from megalon response messages to avro messages
		Map<Class<? extends MegalonMsg>, Class<? extends SpecificRecordBase>> respMsgMap = 
			new HashMap<Class<? extends MegalonMsg>, Class<? extends SpecificRecordBase>>();
		respMsgMap.put(MsgPrepareResp.class, AvroPrepareResponse.class);
		respMsgMap.put(MsgAcceptResp.class, AvroAcceptResponse.class);
		
		avroEncodeStage = new AvroRpcEncode(selectorStage, respMsgMap, 10, 10);
		
		// For the Avro decoder, set up the mapping from message id to avro type
		Map<Byte,Class<? extends SpecificRecordBase>> msgTypes = new 
			HashMap<Byte,Class<? extends SpecificRecordBase>>();
		msgTypes.put(MsgPrepare.MSG_ID, AvroPrepare.class);
		msgTypes.put(MsgAccept.MSG_ID, AvroAccept.class);
		msgTypes.put(MsgChosen.MSG_ID, AvroChosen.class);
		
		// For the Avro decoder, set up the mapping from avro type to megalon type
		Map<Class<? extends SpecificRecordBase>,Class<? extends MegalonMsg>> decClassMap = 
			new HashMap<Class<? extends SpecificRecordBase>,Class<? extends MegalonMsg>>();
		decClassMap.put(AvroPrepare.class, MsgPrepare.class);
		decClassMap.put(AvroAccept.class, MsgAccept.class);
		decClassMap.put(AvroChosen.class, MsgChosen.class);
		
		avroDecodeStage.init(msgTypes, decClassMap, coreServer, execStage, 
				selectorStage, avroEncodeStage, 10, 10);
		
		socketSvrStages.add(selectorStage);
		socketSvrStages.add(avroEncodeStage);
		socketSvrStages.add(avroDecodeStage);
		socketServer = new MultiStageServer<MSocketPayload>("replSocketSvr", 
				socketSvrStages);
		
		paxos = new Client(megalon); 

		
		ready = true; // TODO use this value to prevent premature ops?
		logger.debug("Replication server done with init");
	}
	
	protected void startSocketAccepter() {
		// The socket accepter will send new connections to the socketServer's
		// stage "selectorStage".
		socketAccepter.init(socketServer, null, megalon.config.replsrv_port, 
				selectorStage, new MSocketPayload.Factory(), false);
		Thread accepterThread = new Thread() {
			public void run() {
				try {
					logger.debug("MSocketAccepter starting on port " + 
							megalon.config.replsrv_port);
					socketAccepter.runForever();
				} catch (Exception e) {
					logger.error("ReplServer accepter exception", e);
				}
			}
		};
		accepterThread.setDaemon(true);
		accepterThread.start();
	}
	
	public boolean isReady() {
		return ready;
	}
	
	public void close(SocketPayload sockPayload) {
		try {
			sockPayload.sockChan.close();
		} catch (IOException e) {
			logger.info("IOException closing socket", e);
		}
	}
	
	/**
	 * Given a WALEntry which includes 0 or more Puts and Deletes, apply those
	 * Puts and Deletes to the main database.
	 * 
	 * @return whether the changes were successfully applied.
	 */
	public static boolean applyChanges(Megalon megalon, WALEntry walEntry) {
		Set<CmpBytes> allTableNames = new HashSet<CmpBytes>(walEntry.deletes.keySet());
		allTableNames.addAll(walEntry.puts.keySet());

		// TODO do multiple tables in parallel instead of sequential 
		for(CmpBytes tableName: allTableNames) {
			List<Put> putsThisTable = walEntry.puts.get(tableName);
			List<Delete> deletesThisTable = walEntry.deletes.get(tableName);
			
			int numOpsThisTable = 0;
			if(putsThisTable != null) {
				numOpsThisTable += putsThisTable.size();
			}
			if(deletesThisTable != null) {
				numOpsThisTable += deletesThisTable.size();
			}
			
			List<Row> allOpsThisTable = new ArrayList<Row>(numOpsThisTable);
			if(putsThisTable != null) {
				allOpsThisTable.addAll(putsThisTable);
			}
			if(deletesThisTable != null) {
				allOpsThisTable.addAll(deletesThisTable);
			}
			
			HTableInterface hTable = megalon.getHTablePool().getTable(
					tableName.getBytes());
			try {
				hTable.batch(allOpsThisTable);
			} catch (IOException e) {
				logger.error(e);
				return false;
			} catch (InterruptedException e) {
				logger.error(e);
				return false;
			}
		}
		return true;
	}
}
