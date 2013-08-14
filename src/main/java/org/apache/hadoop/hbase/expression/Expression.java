package org.apache.hadoop.hbase.expression;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.io.Writable;

public abstract interface Expression extends Writable {
	public abstract EvaluationResult evaluate(
			EvaluationContext paramEvaluationContext)
			throws EvaluationException;

	public abstract EvaluationResult.ResultType getReturnType();
}