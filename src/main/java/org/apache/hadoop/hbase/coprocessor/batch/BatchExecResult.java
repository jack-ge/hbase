package org.apache.hadoop.hbase.coprocessor.batch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class BatchExecResult implements Writable {
	private List<byte[]> regionNames;
	private Object value;

	public BatchExecResult() {
	}

	public BatchExecResult(List<byte[]> regions, Object value) {
		this.regionNames = regions;
		this.value = value;
	}

	public List<byte[]> getRegionNames() {
		return this.regionNames;
	}

	public Object getValue() {
		return this.value;
	}

	public void write(DataOutput out) throws IOException {
		int count = this.regionNames.size();
		out.writeInt(count);
		for (int i = 0; i < count; i++) {
			Bytes.writeByteArray(out, (byte[]) this.regionNames.get(i));
		}
		HbaseObjectWritable.writeObject(out, this.value,
				this.value != null ? this.value.getClass() : Writable.class,
				null);
	}

	public void readFields(DataInput in) throws IOException {
		int count = in.readInt();
		this.regionNames = new ArrayList<byte[]>();
		for (int i = 0; i < count; i++) {
			this.regionNames.add(Bytes.readByteArray(in));
		}
		this.value = HbaseObjectWritable.readObject(in, null);
	}
}