package org.megalon;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import org.megalon.Config.Host;

public class Util {
	public static SocketChannel connectTo(Host host) throws IOException {
		InetAddress hostAddr = InetAddress.getByName(host.nameOrAddr);
		InetSocketAddress addr = new InetSocketAddress(hostAddr, host.port);
		return SocketChannel.open(addr);
	}
	
	/**
	 * Turn a long into a big-endian byte array.
	 */
	static public byte[] longToBytes(long l) {
		byte[] bs = new byte[8];
		for(int i=7; i>=0; i--) {
			bs[i] = (byte)l;
			l >>= 8;
		}
		return bs;
	}
	
	/**
	 * Turn a big-endian array of 8 bytes into a long.
	 */
	static public long bytesToLong(byte[] bs) {
		assert bs.length == 8;
			
		long l = 0;
		for(int i=0; i<8; i++) {
			l += bs[i];
			l <<= 8;
		}
		return l;
	}

	/**
	 * Turn an int into a big-endian byte array.
	 */
	static public byte[] intToBytes(int x) {
		byte[] bs = new byte[4];
		for(int i=3; i>=0; i--) {
			bs[i] = (byte)x;
			x >>= 8;
		}
		return bs;
	}
	
	/**
	 * Turn a big-endian array of 8 bytes into a long.
	 */
	static public int bytesToInt(byte[] bs) {
		assert bs.length == 4;
			
		int x = 0;
		for(int i=0; i<4; i++) {
			x <<= 8;
			x += bs[i];
		}
		return x;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	static public ArrayList shuffled(List inputList) {
		ArrayList shuffled = new ArrayList(inputList);
		Collections.shuffle(shuffled);
		return shuffled;
	}
	
	static public String executorStatus(ThreadPoolExecutor exec) {
		return " active=" +	exec.getActiveCount() + 
			" threads=" + exec.getPoolSize() +
			" backlogged=" + exec.getQueue().size() +
			" core=" + exec.getCorePoolSize() +
			" max=" + exec.getMaximumPoolSize();
	}
	
	/**
	 * How many responses (out of x) constitute a quorum?
	 */
	static public int quorum(int x) {
		return x/2 + 1;
	}
	
	/**
	 * How many nacks does it take before a quorum becomes impossible?
	 */
	static public int quorumImpossible(int x) {
		return (x+1) / 2; 
	}
	
	/**
	 * Sleep for some milliseconds, resuming if interrupted.
	 * TODO replace other calls to sleep() with this function
	 */
	static public void sleep(int millis) {
		long startTimeMs = System.currentTimeMillis();
		while(true) {
			try {
				Thread.sleep(millis);
				return;
			} catch (InterruptedException e) {
				long nowTimeMs = System.currentTimeMillis();
				millis -= (nowTimeMs - startTimeMs);
			}
		}
	}
}
