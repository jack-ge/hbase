package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class DoubleColumnInterpreter extends AbstractDoubleColumnInterpreter {
	public Double getValue(byte[] colFamily, byte[] colQualifier, KeyValue kv)
			throws IOException {
		if ((kv == null) || (kv.getValueLength() != 8))
			return null;
		return Double.valueOf(Bytes.toDouble(kv.getBuffer(),
				kv.getValueOffset()));
	}

	public boolean equals(Object obj) {
		return obj.getClass() == DoubleColumnInterpreter.class;
	}
}