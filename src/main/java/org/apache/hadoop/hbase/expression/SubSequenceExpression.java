package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.BytesReference;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;

public class SubSequenceExpression implements Expression {
	private Expression source;
	private Expression start;
	private Expression end;

	public SubSequenceExpression() {
	}

	public SubSequenceExpression(Expression source, int start) {
		this(source, new ConstantExpression(Integer.valueOf(start)), null);
	}

	public SubSequenceExpression(Expression source, int start, int end) {
		this(source, new ConstantExpression(Integer.valueOf(start)),
				new ConstantExpression(Integer.valueOf(end)));
	}

	public SubSequenceExpression(Expression source, Expression start) {
		this(source, start, null);
	}

	public SubSequenceExpression(Expression source, Expression start,
			Expression end) {
		this.source = source;
		this.start = start;
		this.end = end;
	}

	public Expression getSource() {
		return this.source;
	}

	public Expression getStart() {
		return this.start;
	}

	public Expression getEnd() {
		return this.end;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if ((this.source == null) || (this.start == null)) {
			throw new EvaluationException("Missing required arguments");
		}
		EvaluationResult eval = this.source.evaluate(context);
		BytesReference ref = eval.getResultType() == EvaluationResult.ResultType.BYTESREFERENCE ? eval
				.asBytesReference() : null;
		byte[] s = eval.getResultType() == EvaluationResult.ResultType.BYTESREFERENCE ? ref
				.getReference() : eval.asBytes();
		Integer st = this.start.evaluate(context).asInteger();
		Integer e = this.end == null ? null : this.end.evaluate(context)
				.asInteger();
		if ((s == null) || (st == null)) {
			return new EvaluationResult();
		}
		int o = eval.getResultType() == EvaluationResult.ResultType.BYTESREFERENCE ? ref
				.getOffset() : 0;
		int l = eval.getResultType() == EvaluationResult.ResultType.BYTESREFERENCE ? ref
				.getLength() : s.length;
		int len = e == null ? l - st.intValue() : e.intValue() - st.intValue();

		if ((st.intValue() < 0) || (len < 0) || (l < st.intValue() + len)) {
			throw new EvaluationException(new StringBuilder()
					.append("Could not evaluate SubSequenceExpression(")
					.append(Bytes.toString(s)).append(",").append(st)
					.append(",").append(e).append(")").toString());
		}

		byte[] res = new byte[len];
		System.arraycopy(s, st.intValue() + o, res, 0, len);
		return new EvaluationResult(res, EvaluationResult.ResultType.BYTEARRAY);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof SubSequenceExpression)))
			return false;
		SubSequenceExpression other = (SubSequenceExpression) expr;
		return ((this.source == null) && (other.source == null))
				|| ((this.source != null) && (this.source.equals(other.source)) && (((this.start == null) && (other.start == null)) || ((this.start != null)
						&& (this.start.equals(other.start)) && (((this.end == null) && (other.end == null)) || ((this.end != null) && (this.end
						.equals(other.end)))))));
	}

	public int hashCode() {
		int result = this.source == null ? 1 : this.source.hashCode();
		result = result * 31 + (this.start == null ? 1 : this.start.hashCode());
		result = result * 31 + (this.end == null ? 1 : this.end.hashCode());
		return result;
	}

	public String toString() {
		return new StringBuilder()
				.append("subSequence(")
				.append(this.source.toString())
				.append(", ")
				.append(this.start.toString())
				.append(this.end == null ? "" : new StringBuilder()
						.append(", ").append(this.end.toString()).toString())
				.append(")").toString();
	}

	public void readFields(DataInput in) throws IOException {
		this.source = ((Expression) HbaseObjectWritable.readObject(in, null));
		this.start = ((Expression) HbaseObjectWritable.readObject(in, null));
		this.end = ((Expression) HbaseObjectWritable.readObject(in, null));
	}

	public void write(DataOutput out) throws IOException {
		HbaseObjectWritable.writeObject(out, this.source,
				this.source.getClass(), null);
		HbaseObjectWritable.writeObject(out, this.start, this.start.getClass(),
				null);
		HbaseObjectWritable.writeObject(out, this.end,
				this.end == null ? NullWritable.class : this.end.getClass(),
				null);
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.BYTEARRAY;
	}
}