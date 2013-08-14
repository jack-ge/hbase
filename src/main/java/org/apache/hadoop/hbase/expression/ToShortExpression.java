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

public class ToShortExpression implements Expression {
	private Expression subExpression;

	public ToShortExpression() {
	}

	public ToShortExpression(Expression subExpression) {
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
		Short res = null;
		try {
			if (eval.isNullResult()) {
				res = null;
			} else if (eval.isNumber()) {
				res = eval.asShort();
			} else if (eval.getResultType() == EvaluationResult.ResultType.BYTEARRAY) {
				byte[] b = eval.asBytes();
				if ((b.length == 0)
						&& (context.getCompatibilityMode() == EvaluationContext.CompatibilityMode.HIVE))
					res = null;
				else
					res = Short.valueOf(Bytes.toShort(b));
			} else if (eval.getResultType() == EvaluationResult.ResultType.BYTESREFERENCE) {
				BytesReference ref = eval.asBytesReference();
				if ((ref.getLength() == 0)
						&& (context.getCompatibilityMode() == EvaluationContext.CompatibilityMode.HIVE))
					res = null;
				else
					res = Short.valueOf(Bytes.toShort(ref.getReference(),
							ref.getOffset(), ref.getLength()));
			} else if (eval.getResultType() == EvaluationResult.ResultType.STRING) {
				String str = eval.asString();
				if ((str.isEmpty())
						&& (context.getCompatibilityMode() == EvaluationContext.CompatibilityMode.HIVE))
					res = null;
				else
					res = Short.valueOf(str);
			} else {
				throw new TypeConversionException(eval.getResultType(),
						Short.class);
			}
		} catch (Throwable t) {
			if ((t instanceof EvaluationException))
				throw ((EvaluationException) t);
			throw ((EvaluationException) new TypeConversionException(
					eval.getResultType(), Short.class).initCause(t));
		}

		return new EvaluationResult(res, EvaluationResult.ResultType.SHORT);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof ToShortExpression)))
			return false;
		ToShortExpression other = (ToShortExpression) expr;
		return ((this.subExpression == null) && (other.subExpression == null))
				|| ((this.subExpression != null) && (this.subExpression
						.equals(other.subExpression)));
	}

	public int hashCode() {
		return this.subExpression == null ? 1 : this.subExpression.hashCode();
	}

	public String toString() {
		return "toShort(" + this.subExpression.toString() + ")";
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
		return EvaluationResult.ResultType.SHORT;
	}
}