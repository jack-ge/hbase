package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.util.Bytes;

public class ColumnValueExpression implements Expression {
	private byte[] family;
	private byte[] qualifier;

	public ColumnValueExpression() {
	}

	public ColumnValueExpression(byte[] family, byte[] qualifier) {
		this.family = family;
		this.qualifier = qualifier;
	}

	public byte[] getFamily() {
		return this.family;
	}

	public byte[] getQualifier() {
		return this.qualifier;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if ((this.family == null) || (this.qualifier == null)) {
			throw new EvaluationException("Missing required arguments");
		}
		return new EvaluationResult(context.getColumnValue(this.family,
				this.qualifier), EvaluationResult.ResultType.BYTESREFERENCE);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof ColumnValueExpression)))
			return false;
		ColumnValueExpression other = (ColumnValueExpression) expr;
		return (Bytes.equals(this.family, other.family))
				&& (Bytes.equals(this.qualifier, other.qualifier));
	}

	public int hashCode() {
		int result = this.family == null ? 1 : Bytes.hashCode(this.family);
		result = result * 31
				+ (this.qualifier == null ? 1 : Bytes.hashCode(this.qualifier));
		return result;
	}

	public String toString() {
		return "columnValue(\"" + Bytes.toString(this.family) + "\", \""
				+ Bytes.toString(this.qualifier) + "\")";
	}

	public void readFields(DataInput in) throws IOException {
		this.family = Bytes.readByteArray(in);
		this.qualifier = Bytes.readByteArray(in);
	}

	public void write(DataOutput out) throws IOException {
		Bytes.writeByteArray(out, this.family);
		Bytes.writeByteArray(out, this.qualifier);
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.BYTESREFERENCE;
	}
}