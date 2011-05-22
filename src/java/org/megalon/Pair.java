package org.megalon;

public class Pair<S,T> {
	S left;
	T right;
	
	public Pair(S left, T right) {
		this.left = left;
		this.right = right;
	}
	
	public S getLeft() {
		return left;
	}
	
	public T getRight() {
		return right;
	}
}
