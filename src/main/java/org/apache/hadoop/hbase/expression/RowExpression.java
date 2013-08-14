package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;

public class RowExpression implements Expression {
	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		return new EvaluationResult(context.getRow(),
				EvaluationResult.ResultType.BYTESREFERENCE);
	}

	public boolean equals(Object expr) {
		return (expr != null) && ((expr instanceof RowExpression));
	}

	public int hashCode() {
		return 1;
	}

	public String toString() {
		return "row()";
	}

	public void readFields(DataInput in) throws IOException {
	}

	public void write(DataOutput out) throws IOException {
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.BYTESREFERENCE;
	}
}