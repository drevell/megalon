package org.megalon;

import java.util.ArrayList;
import java.util.List;

import org.megalon.avro.AvroWALEntry;
import org.megalon.avro.AvroWALEntryStatus;
import org.megalon.avro.AvroWriteVal;

/**
 * This represents the state of a single entry in the write-ahead log. We could
 * use AvroWALEntry instead, but this class insulates the rest of the program
 * from Avro details. We might want to add other serialization formats in the
 * future.
 */
public class WALEntry {
	public static enum Status {ACCEPTED, CHOSEN, PREPARED};
	
	long n;
	List<SingleWrite> values = null; // TODO nullable values in Avro
	Status status;
	
	public WALEntry(AvroWALEntry avroEntry) {
		this.n = avroEntry.n;
		if(avroEntry.values != null) {
			int numElems = avroEntry.values.size();
			this.values = new ArrayList<SingleWrite>(numElems);
			for(AvroWriteVal avroVal: avroEntry.values) {
				this.values.add(new SingleWrite(avroVal));
			}
		}
		switch(avroEntry.status) {
		case ACCEPTED:
			this.status = Status.ACCEPTED;
			break;
		case CHOSEN:
			this.status = Status.CHOSEN;
			break;
		case PREPARED:
			this.status = Status.PREPARED;
			break;
		}
		//this.walIndex = avroEntry.walIndex;
	}
	
	public WALEntry(long n, List<SingleWrite> values, Status status) {
		this.n = n;
		this.values = values;
		this.status = status;
	}
	
	public AvroWALEntry toAvro() {
		AvroWALEntry avroEntry = new AvroWALEntry();
		avroEntry.n = n;
		if(values != null) {
			avroEntry.values = new ArrayList<AvroWriteVal>(values.size());
			for (SingleWrite write: values) {
				avroEntry.values.add(write.toAvro());
			}
		}
		switch(status) {
		case ACCEPTED:
			avroEntry.status = AvroWALEntryStatus.ACCEPTED;
			break;
		case CHOSEN:
			avroEntry.status = AvroWALEntryStatus.CHOSEN;
			break;
		case PREPARED:
			avroEntry.status = AvroWALEntryStatus.PREPARED;
			break;
		}
//		avroEntry.walIndex = walIndex;
		return avroEntry;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("WALEntry: n=");
		sb.append(n);
		sb.append(", ");
		sb.append(status.toString());
		sb.append(", ");
		if(values != null) {
			for(SingleWrite write: values) {
				sb.append(write.toString());
				sb.append(", ");
			}
		}
		return sb.toString();
	}
}
