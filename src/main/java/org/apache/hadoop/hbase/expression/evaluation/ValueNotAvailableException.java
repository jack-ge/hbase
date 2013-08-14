package org.apache.hadoop.hbase.expression.evaluation;

import org.apache.hadoop.hbase.util.Bytes;

public class ValueNotAvailableException extends EvaluationException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ValueNotAvailableException(String msg) {
		super(msg);
	}

	public ValueNotAvailableException(byte[] family, byte[] column) {
		super("Value of column: " + Bytes.toString(family) + ":"
				+ Bytes.toString(column) + " is not available");
	}
}