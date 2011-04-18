package org.megalon;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.megalon.avro.AvroWriteVal;

public class SingleWrite {
	Log logger = LogFactory.getLog(SingleWrite.class);
	final byte[] table, cf, col, value;
	
	public SingleWrite(AvroWriteVal singleVal) {
		table = new byte[singleVal.table.limit()];
		cf = new byte[singleVal.cf.limit()];
		col = new byte[singleVal.col.limit()];
		value = new byte[singleVal.value.limit()];
		
		singleVal.table.get(this.table);
		singleVal.cf.get(this.cf);
		singleVal.col.get(this.col);
		singleVal.value.get(this.value);
	}
	
	public SingleWrite(byte[] table, byte[] cf, byte[] col, byte[] value) {
		this.table = table;
		this.cf = cf;
		this.col = col;
		this.value = value;
	}
	
	public AvroWriteVal toAvro() {
		AvroWriteVal awv = new AvroWriteVal();
		awv.table = ByteBuffer.wrap(table);
		awv.cf = ByteBuffer.wrap(cf);
		awv.col = ByteBuffer.wrap(col);
		awv.value = ByteBuffer.wrap(value);
		return awv;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(new String(table));
		sb.append(": ");
		sb.append(new String(cf));
		sb.append(": ");
		sb.append(new String(col));
		sb.append(": ");
		sb.append(new String(value));
		
		return sb.toString();
	}
}
