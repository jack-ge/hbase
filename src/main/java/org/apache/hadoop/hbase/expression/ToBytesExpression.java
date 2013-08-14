package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.expression.evaluation.TypeConversionException;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class ToBytesExpression implements Expression {
	private Expression subExpression;

	public ToBytesExpression() {
	}

	public ToBytesExpression(Expression subExpression) {
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
		if (eval.isNullResult()) {
			return new EvaluationResult();
		}
		byte[] res = null;
		try {
			switch (eval.getResultType().ordinal()) {
			case 1:
			case 2:
				res = eval.asBytes();
				break;
			case 3:
				res = new byte[] { eval.asByte().byteValue() };
				break;
			case 4:
				res = Bytes.toBytes(eval.asBigDecimal());
				break;
			case 5:
				res = Bytes.toBytes(eval.asBigDecimal());
				break;
			case 6:
				res = Bytes.toBytes(eval.asBoolean().booleanValue());
				break;
			case 7:
				res = Bytes.toBytes(eval.asDouble().doubleValue());
				break;
			case 8:
				res = Bytes.toBytes(eval.asFloat().floatValue());
				break;
			case 9:
				res = Bytes.toBytes(eval.asInteger().intValue());
				break;
			case 10:
				res = Bytes.toBytes(eval.asLong().longValue());
				break;
			case 11:
				res = Bytes.toBytes(eval.asShort().shortValue());
				break;
			case 12:
				res = Bytes.toBytes(eval.asString());
				break;
			default:
				throw new TypeConversionException(eval.getResultType(),
						byte[].class);
			}
		} catch (Throwable t) {
			if ((t instanceof EvaluationException))
				throw ((EvaluationException) t);
			throw ((EvaluationException) new TypeConversionException(
					eval.getResultType(), byte[].class).initCause(t));
		}

		return new EvaluationResult(res, EvaluationResult.ResultType.BYTEARRAY);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof ToBytesExpression)))
			return false;
		ToBytesExpression other = (ToBytesExpression) expr;
		return ((this.subExpression == null) && (other.subExpression == null))
				|| ((this.subExpression != null) && (this.subExpression
						.equals(other.subExpression)));
	}

	public int hashCode() {
		return this.subExpression == null ? 1 : this.subExpression.hashCode();
	}

	public String toString() {
		return "toBytes(" + this.subExpression.toString() + ")";
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
		return EvaluationResult.ResultType.BYTEARRAY;
	}
}