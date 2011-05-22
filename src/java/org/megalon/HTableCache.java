// This is probably a bad idea.. HBase provides HTablePool
//package org.megalon;
//
///**
// * There are a couple of places in megalon where we want to lazily create and
// * cache HTable connections. We could just rewrite the same HashMap get/set and
// * HTable construction code in each place, but it's friendlier just to put the 
// * code here and give it a friendly reusable interface.
// */
//public class HTableCache {
//	Map<CmpBytes,HTable> cacheMap = new HashMap<CmpBytes,HTable>();
//	
//	public HTable getOrMake(byte[] table) {
//		
//	}
//}
