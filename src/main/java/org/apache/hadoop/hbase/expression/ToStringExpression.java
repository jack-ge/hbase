package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.BytesReference;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.expression.evaluation.TypeConversionException;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class ToStringExpression implements Expression {
	private Expression subExpression;

	public ToStringExpression() {
	}

	public ToStringExpression(Expression subExpression) {
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
		EvaluationResult eval = this.subExpression.evaluate(context);
		String res = null;
		try {
			if (eval.isNullResult()) {
				res = null;
			} else if (eval.getResultType() == EvaluationResult.ResultType.STRING) {
				res = eval.asString();
			} else if (eval.getResultType() == EvaluationResult.ResultType.BIGDECIMAL) {
				res = eval.asBigDecimal().toPlainString();
			} else if (eval.isNumber()) {
				res = eval.asNumber().toString();
			} else if (eval.getResultType() == EvaluationResult.ResultType.BOOLEAN) {
				res = eval.asBoolean().toString();
			} else if (eval.getResultType() == EvaluationResult.ResultType.BYTEARRAY) {
				res = Bytes.toString(eval.asBytes());
			} else if (eval.getResultType() == EvaluationResult.ResultType.BYTESREFERENCE) {
				BytesReference ref = eval.asBytesReference();
				res = Bytes.toString(ref.getReference(), ref.getOffset(),
						ref.getLength());
			} else {
				throw new TypeConversionException(eval.getResultType(),
						String.class);
			}
		} catch (Throwable t) {
			if ((t instanceof EvaluationException))
				throw ((EvaluationException) t);
			throw ((EvaluationException) new TypeConversionException(
					eval.getResultType(), String.class).initCause(t));
		}

		return new EvaluationResult(res, EvaluationResult.ResultType.STRING);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof ToStringExpression)))
			return false;
		ToStringExpression other = (ToStringExpression) expr;
		return ((this.subExpression == null) && (other.subExpression == null))
				|| ((this.subExpression != null) && (this.subExpression
						.equals(other.subExpression)));
	}

	public int hashCode() {
		return this.subExpression == null ? 1 : this.subExpression.hashCode();
	}

	public String toString() {
		return "toString(" + this.subExpression.toString() + ")";
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
		return EvaluationResult.ResultType.STRING;
	}
}