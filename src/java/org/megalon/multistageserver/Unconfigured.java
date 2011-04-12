package org.megalon.multistageserver;

public class Unconfigured extends Exception {
	private static final long serialVersionUID = 1L;

	public Unconfigured() {
		super();
	}

	public Unconfigured(String message, Throwable cause) {
		super(message, cause);
	}

	public Unconfigured(String message) {
		super(message);
	}

	public Unconfigured(Throwable cause) {
		super(cause);
	}	
}
