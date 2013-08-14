package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;

public class BinaryPartialComparator extends WritableByteArrayComparable {
	protected int offset;

	public BinaryPartialComparator() {
	}

	public BinaryPartialComparator(byte[] value, int offset) {
		this.value = value;
		this.offset = offset;
	}

	public byte[] getValue() {
		return this.value;
	}

	public void readFields(DataInput in) throws IOException {
		this.offset = in.readInt();
		this.value = Bytes.readByteArray(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.offset);
		Bytes.writeByteArray(out, this.value);
	}

	public int compareTo(byte[] value, int offset, int length) {
		if (length <= this.offset) {
			return 1;
		}
		return Bytes.compareTo(this.value, 0, this.value.length, value, offset
				+ this.offset,
				value.length <= this.value.length + this.offset ? length
						- this.offset : this.value.length);
	}
}