package org.megalon;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.megalon.Config.ReplicaDesc;

public class CoordCliPayload extends AckPayload {
	// Used for a CheckValid request
	public byte[] entityGroup;
	public String checkValidReplica;
	public boolean egValid; // the answer
	
	// Used for a Validate request
	long walIndex = -1;
	public Map<String,Boolean> validated; // the answer
	List<ReplicaDesc> replicas;
	boolean validate; // true if we're validating, false if we're invalidating
		
	/**
	 * This constructor is used when we're doing a CheckValid operation.
	 */
	public CoordCliPayload(byte[] entityGroup, String replica, long timeoutMs) {
		super(timeoutMs);
		this.entityGroup = entityGroup;
		this.checkValidReplica = replica;
	}

	/**
	 * This constructor is used when we're doing a Validate operation.
	 * @param validate True to validate, false to invalidate
	 */
	public CoordCliPayload(byte[] entityGroup, List<ReplicaDesc> replicas, 
			long timeoutMs, long walIndex, boolean validate) {
		super(timeoutMs);
		this.entityGroup = entityGroup;
		this.replicas = replicas;
		this.walIndex = walIndex;
		this.validated = new HashMap<String,Boolean>();
		this.validate = validate;
	}
	
	public boolean isEgValid() {
		return egValid;
	}
	
	/**
	 * Update the state to show whether a replica successfully validated.
	 */
	public void validate(String replica, boolean didValidate) {
		validated.put(replica, didValidate);
	}
}
