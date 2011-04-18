package org.megalon;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

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
}
