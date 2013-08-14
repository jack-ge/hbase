package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.Text;

public class ArithmeticExpression implements Expression {
	private ArithmeticOperator operator;
	private Expression left;
	private Expression right;

	public ArithmeticExpression() {
		this(ArithmeticOperator.NO_OP, null, null);
	}

	public ArithmeticExpression(ArithmeticOperator operator, Expression left,
			Expression right) {
		this.operator = operator;
		this.left = left;
		this.right = right;
	}

	public ArithmeticOperator getOperator() {
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
		EvaluationResult l = this.left.evaluate(context);
		EvaluationResult r = this.right.evaluate(context);
		if ((l.isNullResult()) || (r.isNullResult())) {
			return new EvaluationResult();
		}
		EvaluationResult res = null;
		switch (this.operator.ordinal()) {
		case 1:
			res = EvaluationResult.numberAdd(l, r);
			break;
		case 2:
			res = EvaluationResult.numberSubtract(l, r);
			break;
		case 3:
			res = EvaluationResult.numberMultiply(l, r);
			break;
		case 4:
			res = EvaluationResult.numberDivide(l, r);
			break;
		case 5:
			res = EvaluationResult.numberRemainder(l, r);
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
		if ((expr == null) || (!(expr instanceof ArithmeticExpression)))
			return false;
		ArithmeticExpression other = (ArithmeticExpression) expr;
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
		this.operator = ArithmeticOperator.valueOf(Text.readString(in));
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
		return (this.left == null) || (this.right == null) ? EvaluationResult.ResultType.BIGDECIMAL
				: EvaluationResult.getMaxResultType(this.left.getReturnType(),
						this.right.getReturnType());
	}

	public static enum ArithmeticOperator {
		ADD(1), SUBTRACT(2), MULTIPLY(3), DIVIDE(4), REMAINDER(5), NO_OP(-1);
		private final int value;

		private ArithmeticOperator(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}
	}
}