package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.util.Bytes;

public class BinarySuffixComparator extends WritableByteArrayComparable {
	public BinarySuffixComparator() {
	}

	public BinarySuffixComparator(byte[] value) {
		super(value);
	}

	public int compareTo(byte[] value, int offset, int length) {
		int startPos = offset + length - this.value.length;
		return Bytes.compareTo(this.value, 0, this.value.length, value,
				startPos >= offset ? startPos : offset,
				startPos >= offset ? this.value.length : length);
	}
}