package org.megalon;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.io.Writable;
import org.megalon.avro.AvroOneTableBatch;
import org.megalon.avro.AvroWALEntry;
import org.megalon.avro.AvroWALEntryStatus;

/**
 * This represents the state of a single entry in the write-ahead log. We could
 * use AvroWALEntry instead, but this class insulates the rest of the program
 * from Avro details. We might want to add other serialization formats in the
 * future.
 */
public class WALEntry {
	public static enum Status {NEW, PREPARED, ACCEPTED, CHOSEN, FLUSHED};
	static Row[] rowExampleArray = new Row[0];
	
	long n;
//	List<SingleWrite> values = null;
	Map<CmpBytes, List<Put>> puts;
	Map<CmpBytes, List<Delete>> deletes;
	Status status;
//	Put[] puts;
//	Delete[] deletes;
	
	public WALEntry(AvroWALEntry avroEntry) throws IOException {
		this.n = avroEntry.n;
		this.puts = new HashMap<CmpBytes, List<Put>>();
		this.deletes = new HashMap<CmpBytes, List<Delete>>();
		
		if(avroEntry.values != null) {
//			this.allWrites = new HashMap<CmpBytes,List<Row>>(numElems);
			for(AvroOneTableBatch avroOneTableBatch: avroEntry.values) {
				assert avroOneTableBatch.table.remaining() == avroOneTableBatch.table.capacity();
				CmpBytes tableName = new CmpBytes(avroOneTableBatch.table.array());

				ByteBuffer serPutList = avroOneTableBatch.puts;
				ByteBuffer serDeleteList = avroOneTableBatch.deletes;
				
				ByteArrayInputStream bbis;
				DataInputStream dis;
				MArrayWritable writablesArray;
				
				if(serPutList != null) {
					bbis = new ByteArrayInputStream(serPutList.array());
					dis = new DataInputStream(bbis);
					writablesArray = new MArrayWritable(Put.class);
					writablesArray.readFields(dis);
					Put[] puts = (Put[])writablesArray.get();
					this.puts.put(tableName, Arrays.asList(puts));
				}
				
				if(serDeleteList != null) {
					bbis = new ByteArrayInputStream(serDeleteList.array());
					dis = new DataInputStream(bbis);
					writablesArray = new MArrayWritable(Delete.class);
					writablesArray.readFields(dis);
					Delete[] deletes = (Delete[])writablesArray.get();
					this.deletes.put(tableName, Arrays.asList(deletes));
				}
			}
		}
		switch(avroEntry.status) {
		case NEW:
			this.status = Status.NEW;
			break;
		case ACCEPTED:
			this.status = Status.ACCEPTED;
			break;
		case CHOSEN:
			this.status = Status.CHOSEN;
			break;
		case PREPARED:
			this.status = Status.PREPARED;
			break;
		case FLUSHED:
			this.status = Status.FLUSHED;
			break;
		}
	}
	
//	public ByteBuffer convertWritesToAvro(Row[] ops, 
//			Class<? extends Writable> opClass) {
//		MArrayWritable arrayWritable = new MArrayWritable(opClass, ops);
//		ByteArrayOutputStream bos = new ByteArrayOutputStream();
//		DataOutputStream dos = new DataOutputStream(bos);
//		try {
//			arrayWritable.write(dos);
//			dos.flush();
//			bos.flush();
//		} catch (IOException e) { // This is inconceivable
//			throw new AssertionError(e);
//		}
//		return ByteBuffer.wrap(bos.toByteArray());
//	}
	
	
	public WALEntry(long n, Map<CmpBytes, List<Put>> puts, 
	Map<CmpBytes, List<Delete>> deletes, Status status) {
		this.puts = puts;
		this.deletes = deletes;
		this.n = n;
		this.status = status;
	}
	
	public WALEntry(Map<CmpBytes, List<Put>> puts, 
			Map<CmpBytes, List<Delete>> deletes) {
		this.puts = puts;
		this.deletes = deletes;
		this.n = -1;
		status = Status.NEW;
	}
	
	public AvroWALEntry toAvro() {
		AvroWALEntry avroEntry = new AvroWALEntry();
		avroEntry.n = n;
		switch(status) {
		case NEW:
			avroEntry.status = AvroWALEntryStatus.NEW;
			break;
		case PREPARED:
			avroEntry.status = AvroWALEntryStatus.PREPARED;
			break;
		case ACCEPTED:
			avroEntry.status = AvroWALEntryStatus.ACCEPTED;
			break;
		case CHOSEN:
			avroEntry.status = AvroWALEntryStatus.CHOSEN;
			break;
		case FLUSHED:
			avroEntry.status = AvroWALEntryStatus.FLUSHED;
			break;

		}
		
		Set<CmpBytes> allTableNames = new HashSet<CmpBytes>();
		if(puts != null) {
			allTableNames.addAll(puts.keySet());
		}
		if(deletes != null) {
			allTableNames.addAll(deletes.keySet());
		}
		
		if(!allTableNames.isEmpty()) {
			avroEntry.values = new LinkedList<AvroOneTableBatch>();
		}
		for(CmpBytes tableName: allTableNames) {
			AvroOneTableBatch avroOneTableBatch = new AvroOneTableBatch();
			avroOneTableBatch.table = ByteBuffer.wrap(tableName.getBytes());
			if(puts != null) {
				List<Put> putsThisTable = puts.get(tableName);
				if(putsThisTable != null) {
					avroOneTableBatch.puts = convertWritesToAvro(putsThisTable, 
							Put.class);
				}
			}
			if(deletes != null) {
				List<Delete> deletesThisTable = deletes.get(tableName);
				if(deletesThisTable != null) {
					avroOneTableBatch.deletes = convertWritesToAvro(deletesThisTable,
							Delete.class);
				}
			}
			avroEntry.values.add(avroOneTableBatch);
		}
		return avroEntry;
	}
	
	public ByteBuffer convertWritesToAvro(List<? extends Writable> ops, 
			Class<? extends Writable> opClass) {
		Writable[] exampleArray = (Writable[])Array.newInstance(opClass, 0);
		Writable[] opsAsArray = ops.toArray(exampleArray);
		MArrayWritable arrayWritable = new MArrayWritable(opClass, opsAsArray);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		try {
			arrayWritable.write(dos);
			dos.flush();
			bos.flush();
		} catch (IOException e) { // This is inconceivable
			throw new AssertionError(e);
		}
		return ByteBuffer.wrap(bos.toByteArray());
	}
	
	
//	ByteBuffer serializeWrites(List writes, Class<? extends Row> listElemClass) {
//		if(allWrites != null) {
//			avroEntry.values = new LinkedList<AvroOneTableBatch>();
//			for(Entry<CmpBytes,List<Row>> e: allWrites.entrySet()) {
//				List<Put> puts = new LinkedList<Put>();
//				List<Delete> deletes = new LinkedList<Delete>();
//				ByteBuffer tableName = ByteBuffer.wrap(e.getKey().getBytes());
//				for(Row queuedOp: e.getValue()) {
//					if(queuedOp instanceof Put) {
//						puts.add((Put)queuedOp);
//					} else {
//						deletes.add((Delete)queuedOp);
//					}
//				}
//				Row[] putsAsRows = (Row[]) (puts.toArray(rowExampleArray));
//				Row[] deletesAsRows = (Row[]) (deletes.toArray(rowExampleArray));
//				ByteBuffer putsSerialized = convertWritesToAvro(putsAsRows, 
//						Put.class);
//				ByteBuffer deletesSerialized = convertWritesToAvro(deletesAsRows,
//						Delete.class);
//				AvroOneTableBatch thisTableBatch = new AvroOneTableBatch();
//				thisTableBatch.deletes = deletesSerialized;
//				thisTableBatch.puts = putsSerialized;
//				thisTableBatch.table = tableName;
//				avroEntry.values.add(thisTableBatch);
//			}
//		}
//		return avroEntry;
//	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("WALEntry: n=");
		sb.append(n);
		sb.append(", ");
		sb.append(status.toString());
		sb.append(", ");
		
		
//		Set<CmpBytes> allTableNames = new HashSet<CmpBytes>();
		if(puts != null) {
			sb.append("puts:");
			sb.append(puts.toString());
//			allTableNames.addAll(puts.keySet());
		}
		if(deletes != null) {
			sb.append(", deletes:");
			sb.append(deletes.toString());
//			allTableNames.addAll(deletes.keySet());
		}
//		
//		for(CmpBytes tableName: allTableNames) {
//			sb.append("Table: " + new String(tableName.getBytes()) +  ": ");
//			
//			if(puts != null) {
//				sb.append("puts: [");
//				List<Put> putsThisTable = puts.get(tableName.getBytes());
//				if(putsThisTable != null) {
//					for(Put put: putsThisTable) {
//						sb.append(put.toString() + ", ");
//					}
//				}
//				sb.append("], ");
//			}
//			if(deletes != null) {
//				sb.append("deletes: [");
//				List<Put> deletesThisTable = puts.get(tableName.getBytes()); 
//				if(deletesThisTable != null) {
//					for(Put put: deletesThisTable) {
//						sb.append(put.toString() + ", ");
//					}
//				}
//				sb.append("]");
//			}
//		}
		return sb.toString();
	}
}
