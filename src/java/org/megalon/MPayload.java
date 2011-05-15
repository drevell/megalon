package org.megalon;

import org.megalon.messages.MegalonMsg;
import org.megalon.multistageserver.Payload;

/**
 * The class of the core replication server and core coordinator
 * payload types. These can be wrapped in a socket payload for use by RPC, or
 * can be used directly when the server is invoked by local function call.
 */
public class MPayload extends Payload {
	public MegalonMsg resp;
	public MegalonMsg req;
	byte msgType;
	
	public MPayload(byte msgType, MegalonMsg req, Payload outerPayload) {
		super(outerPayload);
		this.msgType = msgType;
		this.req = req;
	}
}
