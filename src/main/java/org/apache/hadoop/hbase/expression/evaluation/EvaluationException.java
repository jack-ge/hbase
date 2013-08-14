package org.apache.hadoop.hbase.expression.evaluation;

import org.apache.hadoop.hbase.expression.ExpressionException;

public class EvaluationException extends ExpressionException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3938543899736724947L;

	public EvaluationException(String msg) {
		super(msg);
	}
}