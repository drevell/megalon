package org.megalon;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

/**
 * This is the main application interface to data stored in Megalon. Clients use
 * it to read and write data from Megalon.
 * 
 * Objects of this class are not thread-safe.
 */
public class Client {
	// TODO make these configurable
	static public int DEFAULT_PREPARE_WAIT_MS = 1000;
	static public int DEFAULT_ACCEPT_WAIT_MS = 1000;
	
	Megalon megalon;
	PaxosServer paxosServer;
	ReplicaChooser replSelector = null;
	Log logger = LogFactory.getLog(Client.class);
//	List<SingleWrite> queuedWrites = new LinkedList<SingleWrite>();
//	Map<CmpBytes,List<Row>> queuedWrites = new HashMap<CmpBytes,List<Row>>();
	Map<CmpBytes,List<Put>> queuedPuts = new HashMap<CmpBytes,List<Put>>();
	Map<CmpBytes,List<Delete>> queuedDeletes = new HashMap<CmpBytes,List<Delete>>();
//	Map<CmpBytes,HTable> htables = new HashMap<CmpBytes,HTable>();
	long walIndex;
	Configuration hConf;
	
	public Client(Megalon megalon) throws IOException {
		this.megalon = megalon;
		this.paxosServer = megalon.paxosServer;
		hConf = megalon.getHbaseConf();
		walIndex = -1;
	}
	
	/**
	 * @see {@link #enqueuePut(byte[], Put)}
	 */
	public void put(byte[] table, byte[] row, byte[] family, byte[] qualifier, 
			byte[] val) {
		Put put = new Put(row);
		put.add(family, qualifier, val);
		enqueuePut(table, put);
	}
	
	/**
	 * Do a write as part of this transaction. This does no I/O, but just 
	 * stores the value in memory until commit time. Important note: you will
	 * not observe your own writes in the database until after you commit!
	 */
	public void enqueuePut(byte[] table, Put put) {
		CmpBytes lookupKey = new CmpBytes(table);
		List<Put> putsThisTable = queuedPuts.get(lookupKey);
		if(putsThisTable == null) {
			putsThisTable = new LinkedList<Put>();
			queuedPuts.put(lookupKey, putsThisTable);
		}
		putsThisTable.add(put);
	}
	
	/**
	 * @see {@link #enqueueDelete(byte[], Delete)}
	 */
	public void deleteColumn(byte[] table, byte[] row, byte[] cf, byte[] qualifier) {
		Delete delete = new Delete(row);
		delete.deleteColumn(cf, qualifier);
		enqueueDelete(table, delete);
	}
	
	/**
	 * Do a delete as part of this transaction. This does no I/O, but just 
	 * stores the deletion in memory until commit time. Important note: you will
	 * not observe your own deletes in the database until after you commit!
	 */
	public void enqueueDelete(byte[] table, Delete delete) {
		CmpBytes lookupKey = new CmpBytes(table);
		List<Delete> deletesThisTable = queuedDeletes.get(lookupKey);
		if(deletesThisTable == null) {
			deletesThisTable = new LinkedList<Delete>();
			queuedDeletes.put(lookupKey, deletesThisTable);
		}
		deletesThisTable.add(delete);
	}

	/**
	 * Commit this transaction synchronously (wait for success or failure).
	 * @return whether the transaction committed (if false, the database is 
	 * unchanged).
	 * @param eg The entity group to commit to. Every item written by this
	 * transaction must belong to this entity group!
	 * @param timeoutMs The transaction will fail automatically after this many
	 * milliseconds.
	 */
	public boolean commitSync(byte[] eg, long timeoutMs) throws IOException {
		ensureReplicaSelected();
		try {
			return paxosServer.commit(queuedPuts, queuedDeletes, eg, timeoutMs, 
					walIndex).get();
		} catch (ExecutionException ee) {
			logger.warn(ee);
			return false;
		} catch (InterruptedException ie) {
			logger.warn(ie);
			return false;
		}
	}
	
	/**
	 * Commit this transaction asynchronously. This function will return
	 * immediately. The returned future can be used to tell whether the
	 * transaction was successful, once it completes or times out.
	 * @param eg The entity group to commit to. Every item written by this
	 * transaction must belong to this entity group!
	 * @param timeoutMs The transaction will fail automatically after this many
	 * milliseconds.
	 * @return a Future that will eventually tell whether the transaction
	 * succeeded.
	 */
	public Future<Boolean> commitAsync(byte[] eg, long timeoutMs) 
	throws IOException {
		ensureReplicaSelected();
		// TODO verify existence of cf for put/delete, before committing
		return paxosServer.commit(queuedPuts, queuedDeletes, eg, timeoutMs, 
				walIndex);
	}
	
	
	/** The incoming batch operation list will contain 0 or more Get, Put,
	 * and Delete objects. We want to execute the Gets now and defer the
	 * Puts and Deletes until commit time.
	 * 
	 * We're trying to act like HTable.batch() in our return value. For each 
	 * position in the input list, the same position in the output array will 
	 * contain the result of performing that operation. So if position 5 in the 
	 * input list is a Get, position 5 in the output array will be the result of 
	 * that Get.
	 * 
	 * Since Puts and Deletes aren't executed until later, their positions in
	 * the output array will be null.
	 * TODO: multi-table batches 
	 */
	public void batch(byte[] table, List<Row> ops, Object[] results) 
	throws IOException, InterruptedException {
		assert results.length == ops.size();
		// TODO make sure the incoming ops don't have timestamps
		
		LinkedList<Row> getsOnly = new LinkedList<Row>();
		int numOps = ops.size();
		Iterator<Row> opIter = ops.iterator();
		
		for(int i=0; i<numOps; i++) {
			Row op = opIter.next();
			if(op instanceof Get) {
				getsOnly.addLast((Get)op);
			} else if(op instanceof Put) {
				enqueuePut(table, (Put)op);
			} else if(op instanceof Delete) {
				enqueueDelete(table, (Delete)op);
			}
		}
		
		IOException hbaseIOException = null;
		InterruptedException hbaseInterruptedException = null;
		Object[] getResults = new Object[getsOnly.size()];
		if(getsOnly.size() != 0) {
			// Run the Get operations using the HTable.batch() API.
			HTablePool hTablePool = megalon.getHTablePool();
			HTableInterface hTable = null;
			try {
				ensureReplicaSelected();
				hTable = hTablePool.getTable(table);
				hTable.batch(getsOnly, getResults);
			} catch (IOException e) {
				hbaseIOException = e;
			} catch (InterruptedException e) {
				hbaseInterruptedException = e;
			} finally {
				if(hTable != null) {
					hTablePool.putTable(hTable); // return HTable object to the pool
				}
			}
		}
		
		// Set up the output array. Where the input was a Get, return the result
		// from the HTable.batch() call. Where the input was a Put or Delete,
		// return null.
		opIter = ops.iterator();
		int nextGetResult = 0;
		for(int outResultIdx=0; outResultIdx<numOps; outResultIdx++) {
			if(opIter.next() instanceof Get) {
				results[outResultIdx] = getResults[nextGetResult];
				nextGetResult++;
			} else {
				results[outResultIdx] = null;
			}
		}

		// If the HTable.batch() call raised an exception, re-raise it now.
		if(hbaseIOException != null) {
			throw hbaseIOException;
		} else if(hbaseInterruptedException != null) {
			throw hbaseInterruptedException;
		}
	}
	
	void ensureReplicaSelected() throws IOException {
//		replSelector = ReplicaSelector(megalon);
	}
}
