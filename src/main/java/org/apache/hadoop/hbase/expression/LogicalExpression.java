package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.Text;

public class LogicalExpression implements Expression {
	private LogicalOperator operator;
	private Expression left;
	private Expression right;

	public LogicalExpression() {
		this(LogicalOperator.NO_OP, null, null);
	}

	public LogicalExpression(LogicalOperator operator, Expression left,
			Expression right) {
		this.operator = operator;
		this.left = left;
		this.right = right;
	}

	public LogicalOperator getOperator() {
		return this.operator;
	}

	public Expression getLeft() {
		return this.left;
	}

	public Expression getRight() {
		return this.right;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if ((this.operator == null) || (this.left == null)
				|| (this.right == null)) {
			throw new EvaluationException("Missing required arguments");
		}
		Boolean l = this.left.evaluate(context).asBoolean();
		if (l == null) {
			return new EvaluationResult();
		}
		EvaluationResult res = null;
		switch (this.operator.ordinal()) {
		case 1:
			if (!l.booleanValue())
				res = new EvaluationResult(l,
						EvaluationResult.ResultType.BOOLEAN);
			else {
				res = this.right.evaluate(context);
			}
			break;
		case 2:
			if (l.booleanValue())
				res = new EvaluationResult(l,
						EvaluationResult.ResultType.BOOLEAN);
			else {
				res = this.right.evaluate(context);
			}
			break;
		default:
			throw new EvaluationException("Unsupported operator: "
					+ this.operator);
		}

		return res;
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof LogicalExpression)))
			return false;
		LogicalExpression other = (LogicalExpression) expr;
		boolean b = this.operator == other.operator;
		if (b) {
			if (this.left == null)
				b = other.left == null;
			else {
				b = this.left.equals(other.left);
			}
		}
		if (b) {
			if (this.right == null)
				b = other.right == null;
			else {
				b = this.right.equals(other.right);
			}
		}

		return b;
	}

	public int hashCode() {
		int result = this.operator == null ? 1 : this.operator.hashCode();
		result = result * 31 + (this.left == null ? 1 : this.left.hashCode());
		result = result * 31 + (this.right == null ? 1 : this.right.hashCode());
		return result;
	}

	public String toString() {
		return this.operator.toString().toLowerCase() + "("
				+ this.left.toString() + ", " + this.right.toString() + ")";
	}

	public void readFields(DataInput in) throws IOException {
		this.operator = LogicalOperator.valueOf(Text.readString(in));
		this.left = ((Expression) HbaseObjectWritable.readObject(in, null));
		this.right = ((Expression) HbaseObjectWritable.readObject(in, null));
	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.operator.toString());
		HbaseObjectWritable.writeObject(out, this.left, this.left.getClass(),
				null);
		HbaseObjectWritable.writeObject(out, this.right, this.right.getClass(),
				null);
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.BOOLEAN;
	}

	public static enum LogicalOperator {
		AND, OR, NO_OP;
	}
}