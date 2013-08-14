package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.hbase.expression.evaluation.BytesReference;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.expression.evaluation.TypeConversionException;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class ToBigDecimalExpression implements Expression {
	private Expression subExpression;

	public ToBigDecimalExpression() {
	}

	public ToBigDecimalExpression(Expression subExpression) {
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
		BigDecimal res = null;
		try {
			if (eval.isNullResult()) {
				res = null;
			} else if (eval.isNumber()) {
				res = eval.asBigDecimal();
			} else if (eval.getResultType() == EvaluationResult.ResultType.BYTEARRAY) {
				res = Bytes.toBigDecimal(eval.asBytes());
			} else if (eval.getResultType() == EvaluationResult.ResultType.BYTESREFERENCE) {
				BytesReference ref = eval.asBytesReference();
				res = Bytes.toBigDecimal(ref.getReference(), ref.getOffset(),
						ref.getLength());
			} else {
				throw new TypeConversionException(eval.getResultType(),
						BigDecimal.class);
			}
		} catch (Throwable t) {
			if ((t instanceof EvaluationException))
				throw ((EvaluationException) t);
			throw ((EvaluationException) new TypeConversionException(
					eval.getResultType(), BigDecimal.class).initCause(t));
		}

		return new EvaluationResult(res, EvaluationResult.ResultType.BIGDECIMAL);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof ToBigDecimalExpression)))
			return false;
		ToBigDecimalExpression other = (ToBigDecimalExpression) expr;
		return ((this.subExpression == null) && (other.subExpression == null))
				|| ((this.subExpression != null) && (this.subExpression
						.equals(other.subExpression)));
	}

	public int hashCode() {
		return this.subExpression == null ? 1 : this.subExpression.hashCode();
	}

	public String toString() {
		return "toBigDecimal(" + this.subExpression.toString() + ")";
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
		return EvaluationResult.ResultType.BIGDECIMAL;
	}
}