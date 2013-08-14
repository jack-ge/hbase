package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public class GroupByKeyExpression implements Expression {
	public static int INVALID_KEY_ID = -1;
	private Expression referenceExpression;
	private int keyIndex;

	public GroupByKeyExpression() {
		this(null);
	}

	public GroupByKeyExpression(Expression referenceExpression) {
		this.referenceExpression = referenceExpression;
		this.keyIndex = INVALID_KEY_ID;
	}

	public Expression getReferenceExpression() {
		return this.referenceExpression;
	}

	public int getKeyIndex() {
		return this.keyIndex;
	}

	public void setKeyIndex(int keyIndex) {
		this.keyIndex = keyIndex;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		return context.getGroupByKey(this.keyIndex);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof GroupByKeyExpression)))
			return false;
		GroupByKeyExpression other = (GroupByKeyExpression) expr;
		return ((this.referenceExpression == null) && (other.referenceExpression == null))
				|| ((this.referenceExpression != null) && (this.referenceExpression
						.equals(other.referenceExpression)));
	}

	public int hashCode() {
		return this.referenceExpression == null ? 1 : this.referenceExpression
				.hashCode();
	}

	public String toString() {
		return "groupByKey(" + this.referenceExpression.toString() + ")";
	}

	public void readFields(DataInput in) throws IOException {
		this.referenceExpression = ((Expression) HbaseObjectWritable
				.readObject(in, null));
		this.keyIndex = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		HbaseObjectWritable.writeObject(out, this.referenceExpression,
				this.referenceExpression.getClass(), null);
		out.writeInt(this.keyIndex);
	}

	public EvaluationResult.ResultType getReturnType() {
		return this.referenceExpression.getReturnType();
	}
}