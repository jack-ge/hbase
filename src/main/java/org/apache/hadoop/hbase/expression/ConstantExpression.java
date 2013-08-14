package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class ConstantExpression implements Expression {
	private Object constant;
	private transient EvaluationResult.ResultType type;

	public ConstantExpression() {
		this(null);
	}

	public ConstantExpression(Object constant) {
		this.constant = constant;
		this.type = EvaluationResult.getObjectResultType(constant);
	}

	public Object getConstant() {
		return this.constant;
	}

	public EvaluationResult.ResultType getType() {
		return this.type;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		return new EvaluationResult(this.constant, this.type);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof ConstantExpression)))
			return false;
		ConstantExpression other = (ConstantExpression) expr;
		return ((this.constant == null) && (other.constant == null))
				|| ((this.constant != null) && (other.constant != null)
						&& ((this.constant instanceof byte[]))
						&& ((other.constant instanceof byte[])) && (Bytes
							.equals((byte[]) (byte[]) this.constant,
									(byte[]) (byte[]) other.constant)))
				|| (this.constant.equals(other.constant));
	}

	public int hashCode() {
		return (this.constant instanceof byte[]) ? Bytes
				.hashCode((byte[]) (byte[]) this.constant)
				: this.constant == null ? 1 : this.constant.hashCode();
	}

	public String toString() {
		return "constant("
				+ new EvaluationResult(this.constant, this.type).toString()
				+ ")";
	}

	public void readFields(DataInput in) throws IOException {
		this.constant = HbaseObjectWritable.readObject(in, null);
		this.type = EvaluationResult.getObjectResultType(this.constant);
	}

	public void write(DataOutput out) throws IOException {
		HbaseObjectWritable.writeObject(
				out,
				this.constant,
				this.constant == null ? Writable.class : this.constant
						.getClass(), null);
	}

	public EvaluationResult.ResultType getReturnType() {
		return this.type;
	}
}