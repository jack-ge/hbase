package org.apache.hadoop.hbase.client.coprocessor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class CompositeDoubleStrColumnInterpreter extends
		AbstractDoubleColumnInterpreter {
	private String delimiter = ",";
	private int index = 0;
	private Pattern pattern = null;

	public CompositeDoubleStrColumnInterpreter() {
	}

	public CompositeDoubleStrColumnInterpreter(String delimiter, int index) {
		this.delimiter = delimiter;
		this.index = index;
	}

	public Double getValue(byte[] colFamily, byte[] colQualifier, KeyValue kv)
			throws IOException {
		if (kv == null) {
			return null;
		}
		String val = Bytes.toString(kv.getBuffer(), kv.getValueOffset(),
				kv.getValueLength());

		if (val == null) {
			return null;
		}
		if (this.index < 0) {
			return null;
		}
		if (this.pattern == null) {
			this.pattern = Pattern.compile(this.delimiter);
		}
		String[] parts = this.pattern.split(val, this.index + 2);
		if (parts.length <= this.index) {
			return null;
		}
		Double result = null;
		try {
			val = parts[this.index];
			result = Double.valueOf(val);
		} catch (NumberFormatException e) {
		}
		return result;
	}

	public String getDelimiter() {
		return this.delimiter;
	}

	public int getIndex() {
		return this.index;
	}

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.delimiter = in.readUTF();
		this.index = in.readInt();
		this.pattern = null;
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeUTF(this.delimiter);
		out.writeInt(this.index);
	}

	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if ((obj instanceof CompositeDoubleStrColumnInterpreter)) {
			CompositeDoubleStrColumnInterpreter another = (CompositeDoubleStrColumnInterpreter) obj;
			return (this.delimiter.equals(another.getDelimiter()))
					&& (this.index == another.getIndex());
		}

		return false;
	}
}