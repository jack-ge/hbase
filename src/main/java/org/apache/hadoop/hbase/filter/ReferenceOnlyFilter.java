package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;

public class ReferenceOnlyFilter extends FilterBase {
	public Filter.ReturnCode filterKeyValue(KeyValue v) {
		if ((null != v) && (v.getType() == KeyValue.Type.Reference.getCode())) {
			return Filter.ReturnCode.INCLUDE;
		}
		return Filter.ReturnCode.SKIP;
	}

	public void write(DataOutput out) throws IOException {
	}

	public void readFields(DataInput in) throws IOException {
	}
}