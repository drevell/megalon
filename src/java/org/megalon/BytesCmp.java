package org.megalon;

import java.util.Arrays;

public class BytesCmp {
	byte[] bytes;
	
	public BytesCmp(byte[] bytes) {
		this.bytes = bytes;
	}

	public boolean equals(Object obj) {
		if(!(obj instanceof BytesCmp)) {
			return false;
		}
		return Arrays.equals(bytes, ((BytesCmp)obj).bytes);
	}

	public int hashCode() {
		int x = 1;
		for(int i=0; i<bytes.length; i++) {
			x *= (int)bytes[i] << i;
		}
		return x;
	}
}
