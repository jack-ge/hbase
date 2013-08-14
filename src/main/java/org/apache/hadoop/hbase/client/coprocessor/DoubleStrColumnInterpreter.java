package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class DoubleStrColumnInterpreter extends AbstractDoubleColumnInterpreter {
	public Double getValue(byte[] colFamily, byte[] colQualifier, KeyValue kv)
			throws IOException {
		if (kv == null) {
			return null;
		}
		String val = Bytes.toString(kv.getBuffer(), kv.getValueOffset(),
				kv.getValueLength());
		Double result = null;
		try {
			result = Double.valueOf(val);
		} catch (NumberFormatException e) {
		}
		return result;
	}

	public boolean equals(Object obj) {
		return obj.getClass() == DoubleStrColumnInterpreter.class;
	}
}