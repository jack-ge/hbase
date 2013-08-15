package org.apache.hadoop.hbase.mq;

import java.util.List;

public class Response {
	private Offset nextOffset = null;
	private List<Message> messages = null;

	public Response(List<Message> messages, Offset nextOffset) {
		this.messages = messages;
		this.nextOffset = nextOffset;
	}

	public Offset nextOffset() {
		return this.nextOffset;
	}

	public List<Message> getMessage() {
		return this.messages;
	}
}