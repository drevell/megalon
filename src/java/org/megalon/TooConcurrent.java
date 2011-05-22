package org.megalon;

import java.io.IOException;

/**
 * This is thrown when a CAS transaction has retried the maximum number of 
 * times. It means that there is too much concurrent access to a single
 * transactional domain (aka entity group). Optimistic concurrency control
 * assumes that there is not much concurrent access to a single group.
 */
public class TooConcurrent extends IOException {
	private static final long serialVersionUID = 1L;
	
	public TooConcurrent() {
		super();
	}
	
	public TooConcurrent(Throwable e) {
		super(e);
	}
	
	public TooConcurrent(String msg) {
		super(msg);
	}
}
