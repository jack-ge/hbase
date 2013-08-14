package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public class NotExpression implements Expression {
	private Expression subExpression;

	public NotExpression() {
	}

	public NotExpression(Expression subExpression) {
		this.subExpression = subExpression;
	}

	public Expression getSubExpression() {
		return this.subExpression;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if (this.subExpression == null) {
			throw new EvaluationException("Missing required arguments");
		}
		Boolean b = this.subExpression.evaluate(context).asBoolean();
		if (b == null) {
			return new EvaluationResult();
		}
		return new EvaluationResult(Boolean.valueOf(!b.booleanValue()),
				EvaluationResult.ResultType.BOOLEAN);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof NotExpression)))
			return false;
		NotExpression other = (NotExpression) expr;
		return ((this.subExpression == null) && (other.subExpression == null))
				|| ((this.subExpression != null) && (this.subExpression
						.equals(other.subExpression)));
	}

	public int hashCode() {
		return this.subExpression == null ? 1 : this.subExpression.hashCode();
	}

	public String toString() {
		return "not(" + this.subExpression.toString() + ")";
	}

	public void readFields(DataInput in) throws IOException {
		this.subExpression = ((Expression) HbaseObjectWritable.readObject(in,
				null));
	}

	public void write(DataOutput out) throws IOException {
		HbaseObjectWritable.writeObject(out, this.subExpression,
				this.subExpression.getClass(), null);
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.BOOLEAN;
	}
}