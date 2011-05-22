package org.megalon;

import java.util.Arrays;

/**
 * This is a hack that allows us to use a byte array as a hashmap key. We just
 * have to implement hashCode() and equals() in a way that uses the contents of
 * the array rather than its object identity.
 */
public class CmpBytes {
	byte[] bytes;
	
	public CmpBytes(byte[] bytes) {
		this.bytes = bytes;
	}
	
	public byte[] getBytes() {
		return bytes;
	}

	public boolean equals(Object obj) {
		if(!(obj instanceof CmpBytes)) {
			return false;
		}
		return Arrays.equals(this.bytes, ((CmpBytes)obj).bytes);
	}

	public int hashCode() {
		return Arrays.hashCode(bytes);
	}
}
