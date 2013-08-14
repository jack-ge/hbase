package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public class TernaryExpression implements Expression {
	private Expression condExpression;
	private Expression trueExpression;
	private Expression falseExpression;

	public TernaryExpression() {
	}

	public TernaryExpression(Expression condExpression,
			Expression trueExpression, Expression falseExpression) {
		this.condExpression = condExpression;
		this.trueExpression = trueExpression;
		this.falseExpression = falseExpression;
	}

	public Expression getCondExpression() {
		return this.condExpression;
	}

	public Expression getTrueExpression() {
		return this.trueExpression;
	}

	public Expression getFalseExpression() {
		return this.falseExpression;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if ((this.condExpression == null) || (this.trueExpression == null)
				|| (this.falseExpression == null)) {
			throw new EvaluationException("Missing required arguments");
		}
		Boolean cond = this.condExpression.evaluate(context).asBoolean();
		if (cond == null) {
			return new EvaluationResult();
		}
		return cond.booleanValue() ? this.trueExpression.evaluate(context)
				: this.falseExpression.evaluate(context);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof TernaryExpression)))
			return false;
		TernaryExpression other = (TernaryExpression) expr;
		return ((this.condExpression == null) && (other.condExpression == null))
				|| ((this.condExpression != null)
						&& (this.condExpression.equals(other.condExpression)) && (((this.trueExpression == null) && (other.trueExpression == null)) || ((this.trueExpression != null)
						&& (this.trueExpression.equals(other.trueExpression)) && (((this.falseExpression == null) && (other.falseExpression == null)) || ((this.falseExpression != null) && (this.falseExpression
						.equals(other.falseExpression)))))));
	}

	public int hashCode() {
		int result = this.condExpression == null ? 1 : this.condExpression
				.hashCode();
		result = result
				* 31
				+ (this.trueExpression == null ? 1 : this.trueExpression
						.hashCode());
		result = result
				* 31
				+ (this.falseExpression == null ? 1 : this.falseExpression
						.hashCode());
		return result;
	}

	public String toString() {
		return "if(" + this.condExpression.toString() + ") then ("
				+ this.trueExpression.toString() + ") else ("
				+ this.falseExpression.toString() + ")";
	}

	public void readFields(DataInput in) throws IOException {
		this.condExpression = ((Expression) HbaseObjectWritable.readObject(in,
				null));
		this.trueExpression = ((Expression) HbaseObjectWritable.readObject(in,
				null));
		this.falseExpression = ((Expression) HbaseObjectWritable.readObject(in,
				null));
	}

	public void write(DataOutput out) throws IOException {
		HbaseObjectWritable.writeObject(out, this.condExpression,
				this.condExpression.getClass(), null);
		HbaseObjectWritable.writeObject(out, this.trueExpression,
				this.trueExpression.getClass(), null);
		HbaseObjectWritable.writeObject(out, this.falseExpression,
				this.falseExpression.getClass(), null);
	}

	public EvaluationResult.ResultType getReturnType() {
		EvaluationResult.ResultType trueType = this.trueExpression == null ? EvaluationResult.ResultType.UNKNOWN
				: this.trueExpression.getReturnType();
		EvaluationResult.ResultType falseType = this.falseExpression == null ? EvaluationResult.ResultType.UNKNOWN
				: this.falseExpression.getReturnType();
		return falseType == EvaluationResult.ResultType.UNKNOWN ? trueType
				: trueType == EvaluationResult.ResultType.UNKNOWN ? falseType
						: EvaluationResult
								.getMaxResultType(trueType, falseType);
	}
}