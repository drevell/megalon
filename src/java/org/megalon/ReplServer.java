package org.megalon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.avro.AvroAccept;
import org.megalon.avro.AvroAcceptResponse;
import org.megalon.avro.AvroPrepare;
import org.megalon.avro.AvroPrepareResponse;
import org.megalon.multistageserver.MultiStageServer;
import org.megalon.multistageserver.MultiStageServer.StageDesc;
import org.megalon.multistageserver.SocketAccepter;

/**
 * This class implements the Replication Server component of megalon. It
 * does the same things as the Replication Server in Megastore (Paxos accepter,
 * apply changes to the WAL & main DB).
 */
public class ReplServer {
	public static final int STAGE_ID = 1;
	
	// TODO use config
	public static final int CONCURRENT_CLIENTS = 1000;
	
	static Log logger = LogFactory.getLog(ReplServer.class);
	Megalon megalon;
	InetSocketAddress proxyTo;
	WAL wal;
	MultiStageServer<MPayload> server;
	List<SocketAccepter<MPayload>> socketAccepters = 
		new LinkedList<SocketAccepter<MPayload>>();
	Lock socketAcceptersLock = new ReentrantLock();
	boolean ready = false;
	
	public ReplServer(Megalon megalon) {
		this.megalon = megalon;
		Map<Integer,StageDesc<MPayload>> stages = 
			new HashMap<Integer,StageDesc<MPayload>>();
		StageDesc<MPayload> stageDesc = new StageDesc<MPayload>(
				CONCURRENT_CLIENTS, new ReplServerStage(), "repl_server");
		stages.put(STAGE_ID, stageDesc);
		server = new MultiStageServer<MPayload>(stages);
	}
	
	public void init() throws IOException {
		logger.debug("Replication server starting");
		wal = new WAL(megalon);
		ready = true; // TODO use this value to prevent premature ops?
	}
	
	protected void startSocketAccepter() {
		final SocketAccepter<MPayload> accepter = new MSocketAccepter(server,
				null, megalon.config.replsrv_port, STAGE_ID);
		Thread accepterThread = new Thread() {
			public void run() {
				try {
					accepter.runForever();
				} catch (Exception e) {
					logger.warn("ReplServer accepter exception", e);
				}
			}
		};
		accepterThread.start();
		try {
			socketAcceptersLock.lock();
			socketAccepters.add(accepter);
		} finally {
			socketAcceptersLock.unlock();
		}
	}
	
	public boolean isReady() {
		return ready;
	}
	
	class ReplServerStage implements MultiStageServer.Stage<MPayload> {
		final DatumReader<AvroPrepare> prepareReader = 
			new SpecificDatumReader<AvroPrepare>(AvroPrepare.class);
		final DatumWriter<AvroPrepareResponse> prepareRespWriter = new 
			SpecificDatumWriter<AvroPrepareResponse>(AvroPrepareResponse.class);
		final DatumReader<AvroAccept> acceptReader = 
			new SpecificDatumReader<AvroAccept>(AvroAccept.class);
		final DatumWriter<AvroAcceptResponse> acceptRespWriter = 
			new SpecificDatumWriter<AvroAcceptResponse>(AvroAcceptResponse.class);
		
		public int runStage(MPayload payload) throws Exception {
			// TODO it would be nice if we didn't use Avro directly here. It
			// would be better to have some general system where any
			// serialization system could be plugged in.
			InputStream is = payload.sockChan.socket().getInputStream();
			OutputStream os = payload.sockChan.socket().getOutputStream();
//			ByteBuffer bb = ByteBuffer.allocate(4096);
//			payload.sockChan.read(bb);
//			bb.flip();
//			ByteArrayInputStream bis = new ByteArrayInputStream(bb.array());
			Decoder dec = DecoderFactory.get().binaryDecoder(is, null);
			Encoder enc = EncoderFactory.get().binaryEncoder(os, null);
			while(true) {
				byte[] msgType = new byte[1];
				is.read(msgType, 0, 1);
				// TODO don't do IO here, parse message and pass to next stage
				switch(msgType[0]) {
				
				case PaxosSocketMultiplexer.MSG_PREPARE:
					AvroPrepare avroPrepare = prepareReader.read(null, dec);
					WALEntry result = 
						wal.prepareLocal(avroPrepare.walIndex, avroPrepare.n);
					AvroPrepareResponse avResp = new AvroPrepareResponse();
					avResp.reqSerial = avroPrepare.reqSerial;
					if(result == null) {
						avResp.walEntry = null;
					} else {
						avResp.walEntry = result.toAvro();
					}
					prepareRespWriter.write(avResp, enc);
					break;
				case PaxosSocketMultiplexer.MSG_ACCEPT:
					AvroAccept avroAccept = acceptReader.read(null, dec);
					WALEntry walEntry = new WALEntry(avroAccept.walEntry);
					AvroAcceptResponse response = new AvroAcceptResponse();
					response.reqSerial = avroAccept.reqSerial;
					response.acked = wal.acceptLocal(avroAccept.walIndex, 
							walEntry);
					acceptRespWriter.write(response, enc);
					break;
				default:
					logger.warn("ReplServer received unknown msg type, " +
							"closing connection");
					return -1; // payload finalizer will close the socket
				}
			}
		}
	}
	
	public static class ReplServerStatus {
		
	}
}
