package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public class InExpression implements Expression {
	private Expression testExpression;
	private Set<EvaluationResult> valueSet;

	public InExpression() {
	}

	public InExpression(Expression testExpression,
			ConstantExpression[] constantExpressions) {
		this.testExpression = testExpression;
		this.valueSet = new TreeSet<EvaluationResult>(
				EvaluationResult.NULL_AS_MIN_COMPARATOR);

		for (ConstantExpression expr : constantExpressions)
			this.valueSet.add(new EvaluationResult(expr.getConstant(), expr
					.getType()));
	}

	public InExpression(Expression testExpression,
			Set<EvaluationResult> valueSet) {
		this.testExpression = testExpression;
		this.valueSet = valueSet;
	}

	public Expression getTestExpression() {
		return this.testExpression;
	}

	public Set<EvaluationResult> getValueSet() {
		return this.valueSet;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if ((this.testExpression == null) || (this.valueSet == null)) {
			throw new EvaluationException("Missing required arguments");
		}
		EvaluationResult eval = this.testExpression.evaluate(context);
		return new EvaluationResult(Boolean.valueOf(this.valueSet
				.contains(eval)), EvaluationResult.ResultType.BOOLEAN);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof InExpression)))
			return false;
		InExpression other = (InExpression) expr;
		return ((this.testExpression == null) && (other.testExpression == null))
				|| ((this.testExpression != null)
						&& (this.testExpression.equals(other.testExpression)) && (((this.valueSet == null) && (other.valueSet == null)) || ((this.valueSet != null) && (this.valueSet
						.equals(other.valueSet)))));
	}

	public int hashCode() {
		int result = this.testExpression == null ? 1 : this.testExpression
				.hashCode();
		result = result * 31
				+ (this.valueSet == null ? 1 : this.valueSet.hashCode());
		return result;
	}

	public String toString() {
		String res = "in(" + this.testExpression.toString();
		for (EvaluationResult value : this.valueSet) {
			res = res + ", " + value.toString();
		}
		return res + ")";
	}

	public void readFields(DataInput in) throws IOException {
		this.testExpression = ((Expression) HbaseObjectWritable.readObject(in,
				null));
		int count = in.readInt();
		if (count < 0) {
			this.valueSet = null;
			return;
		}
		this.valueSet = new TreeSet<EvaluationResult>(
				EvaluationResult.NULL_AS_MIN_COMPARATOR);
		for (int i = 0; i < count; i++)
			this.valueSet.add((EvaluationResult) HbaseObjectWritable
					.readObject(in, null));
	}

	public void write(DataOutput out) throws IOException {
		HbaseObjectWritable.writeObject(out, this.testExpression,
				this.testExpression.getClass(), null);
		if (this.valueSet == null) {
			out.writeInt(-1);
			return;
		}
		out.writeInt(this.valueSet.size());
		for (EvaluationResult value : this.valueSet)
			HbaseObjectWritable.writeObject(out, value, value.getClass(), null);
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.BOOLEAN;
	}
}