package org.megalon;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.megalon.avro.AvroWALEntry;

public class WAL {
	public static final byte[] ENTRY_BYTES = "entry".getBytes();
	public static final byte[] N_BYTES = "n".getBytes();
	public static final int MAX_ACCEPT_TRIES = 5;
	public static final int MAX_PREPARE_TRIES = 5;
	
	// TODO don't hardcode
	byte[] cf = "CommitLogCF".getBytes();
	
	Log logger = LogFactory.getLog(WAL.class);
	final DatumReader<AvroWALEntry> reader = 
		new SpecificDatumReader<AvroWALEntry>(AvroWALEntry.class);
	final DatumWriter<AvroWALEntry> writer = 
		new SpecificDatumWriter<AvroWALEntry>(AvroWALEntry.class); 
	final Configuration hconf;
	HTable htable;
	Megalon megalon;

	public WAL(Megalon megalon) throws IOException {
		this.megalon = megalon;
		hconf = HBaseConfiguration.create(); // TODO HBase address
//		hconf.set("hbase.zookeeper.quorum", commaSepZkServers);
		htable = new HTable(hconf, "CommitLogTable");
	}
	
	public WALEntry read(long walIndex) throws IOException {
		Get get = new Get(Util.longToBytes(walIndex));
		get.addColumn(cf, ENTRY_BYTES);

		Result result = htable.get(get);
		KeyValue columnKv = result.getColumnLatest(cf, ENTRY_BYTES);

		if(columnKv != null) {
			byte[] valBytes = columnKv.getValue();
			Decoder decoder = DecoderFactory.get().binaryDecoder(valBytes, null);
			AvroWALEntry prevEntry = reader.read(null, decoder);
			return new WALEntry(prevEntry);
		} else {
			return null;
		}
	}
	
	/**
	 * Looks at the WAL in HBase to get the most recent data for a certain WAL
	 * entry, identified by the proposal number. If there was a previously
	 * accepted value for this log entry, it would be returned, and nothing
	 * will be written. If there was a previously "promised" n, and the caller's
	 * n is greater, then the caller's n will be 
	 * 
	 * @param n
	 * @return null: Paxos ack of prepare message. Non-null WALEntry: the
	 * existing log message, which also means Paxos NACK. 
	 * @throws IOException
	 */
	public WALEntry prepareLocal(long walIndex, long n) 
	throws IOException {
		int tries = 0;
		while(true) {
			try {
				WALEntry existingEntry = read(walIndex);
				// Read the preexisting log entry, see if we should supersede it
				if(existingEntry != null) {
					if(!(existingEntry.status == WALEntry.Status.PREPARED && 
							existingEntry.n < n)) {
						return existingEntry;
					}
				}
				
				// We haven't seen anything yet for this WAL entry
				WALEntry newEntry = new WALEntry(n, null, 
						WALEntry.Status.PREPARED);
			
				putWAL(walIndex, newEntry, existingEntry); 
				return null;
			} catch (CasChanged e) {
				tries++;
				if(tries >= MAX_PREPARE_TRIES) {
					logger.debug("acceptLocal too many CAS retries");
					throw new TooConcurrent();
				}
				logger.debug("acceptLocal CAS failed, retrying");
			}
		}
	}
	
	/**
	 * Write an entry to the WAL in HBase.
	 */
	protected void putWAL(long walIndex, WALEntry entry, WALEntry casCheck) 
	throws IOException {
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		BinaryEncoder e = EncoderFactory.get().binaryEncoder(bao, null);
		byte[] walIndexBytes = Util.longToBytes(walIndex);
		
		writer.write(entry.toAvro(), e);
		e.flush();
		byte[] serializedWALEntry = bao.toByteArray();
		
		Put put = new Put(walIndexBytes);
		put.add(cf, ENTRY_BYTES, serializedWALEntry);
		put.add(cf, N_BYTES, Util.longToBytes(entry.n));
		if(casCheck != null) {
			// Atomic compare and swap: write the new value if and only if the
			// n value hasn't changed since we read it.
			byte[] casN = Util.longToBytes(casCheck.n);
			if(!htable.checkAndPut(walIndexBytes, cf, N_BYTES, casN, put)) {
				throw new CasChanged();
			}
		} else {
			htable.put(put);
		}
	}
	
	/**
	 * Attempt a Paxos accept on behalf of the local data center.
	 * @return true if the entry had the highest n seen yet and was written to
	 * the WAL. false otherwise.
	 */
	protected boolean acceptLocal(long walIndex, WALEntry entry) 
	throws IOException {
		int tries = 0;
		while(true) {
			try {
				WALEntry existingEntry = read(walIndex);
				if(existingEntry != null && existingEntry.n > entry.n) {
					return false;
				} else {
					putWAL(walIndex, entry, existingEntry);
					return true;
				}
			} catch (CasChanged e) {
				tries++;
				if(tries >= MAX_ACCEPT_TRIES) {
					logger.debug("acceptLocal too many CAS retries");
					throw new TooConcurrent();
				}
				logger.debug("acceptLocal CAS failed, retrying");
			}
		}
	}
}
