package org.megalon;

import java.io.IOException;

/**
 * This exception is thrown when a compare-and-swap operation aborts because
 * the compare step saw that the value had changed. To implement optimistic 
 * concurrency control, this exception is caught and the entire operation is 
 * retried, including all reads.
 */
public class CasChanged extends IOException {
	private static final long serialVersionUID = 1L;
}
