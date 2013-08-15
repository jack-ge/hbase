package org.apache.hadoop.hbase.mq;

import java.util.Date;
import org.apache.hadoop.hbase.util.Bytes;

public class Offset implements Comparable<Offset> {
	private byte[] offset;

	public static Offset createFirstOffsetOnTime(Date date) {
		return new Offset(Bytes.toBytes(date.getTime()));
	}

	public Offset(byte[] offset) {
		if (null == offset) {
			throw new IllegalArgumentException();
		}
		this.offset = offset;
	}

	public byte[] get() {
		return this.offset;
	}

	public int compareTo(Offset o) {
		if (null == o) {
			return 1;
		}
		return Bytes.compareTo(this.offset, o.offset);
	}
}