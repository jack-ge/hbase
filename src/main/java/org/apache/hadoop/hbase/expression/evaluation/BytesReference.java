package org.apache.hadoop.hbase.expression.evaluation;

import org.apache.hadoop.hbase.util.Bytes;

public class BytesReference implements Comparable<BytesReference> {
	private final byte[] reference;
	private final int offset;
	private final int length;

	public BytesReference(byte[] reference, int offset, int length) {
		this.reference = reference;
		this.offset = offset;
		this.length = length;
	}

	public byte[] getReference() {
		return this.reference;
	}

	public int getOffset() {
		return this.offset;
	}

	public int getLength() {
		return this.length;
	}

	public byte[] toBytes() {
		byte[] res = new byte[this.length];
		System.arraycopy(this.reference, this.offset, res, 0, this.length);
		return res;
	}

	public boolean equals(Object arg) {
		if ((arg == null) || (!(arg instanceof BytesReference))) {
			return false;
		}
		BytesReference other = (BytesReference) arg;
		return (Bytes.equals(this.reference, other.reference))
				&& (this.offset == other.offset)
				&& (this.length == other.length);
	}

	public int hashCode() {
		if (this.reference == null) {
			return 1;
		}
		return Bytes.hashCode(this.reference, this.offset, this.length);
	}

	public int compareTo(BytesReference other) {
		if (this.reference == null)
			return other.reference == null ? 0 : -1;
		if (other.reference == null) {
			return 1;
		}
		return Bytes.compareTo(this.reference, this.offset, this.length,
				other.reference, other.offset, other.length);
	}
}