package org.apache.hadoop.hbase.mq;

import org.apache.hadoop.hbase.util.Bytes;

public class Message {
	private byte[] buffer;

	public Message(byte[] msg) {
		this.buffer = msg;
	}

	public Message(String message) {
		this.buffer = Bytes.toBytes(message);
	}

	public byte[] getBuffer() {
		return this.buffer;
	}

	public String toString() {
		if (null == this.buffer) {
			return null;
		}
		StringBuilder builder = new StringBuilder();
		builder.append(Bytes.toStringBinary(this.buffer));
		return builder.toString();
	}
}