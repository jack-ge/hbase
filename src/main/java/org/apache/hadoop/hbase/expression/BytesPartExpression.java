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

public class BytesPartExpression implements Expression {
	private Expression source;
	private byte[] delimiter;
	private Expression index;

	public BytesPartExpression() {
	}

	public BytesPartExpression(Expression source, String delimiter, int index) {
		this(source, delimiter, new ConstantExpression(Integer.valueOf(index)));
	}

	public BytesPartExpression(Expression source, String delimiter,
			Expression index) {
		this.source = source;
		this.delimiter = (delimiter == null ? null : Bytes.toBytes(delimiter));
		this.index = index;
	}

	public Expression getSource() {
		return this.source;
	}

	public String getDelimiter() {
		return Bytes.toString(this.delimiter);
	}

	public Expression getIndex() {
		return this.index;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if ((this.source == null) || (this.delimiter == null)
				|| (null == this.delimiter) || (this.delimiter.length == 0)
				|| (this.index == null)) {
			throw new EvaluationException("Missing required arguments");
		}
		BytesReference s = this.source.evaluate(context).asBytesReference();
		Integer i = this.index.evaluate(context).asInteger();

		if ((s == null) || (i == null)) {
			return new EvaluationResult();
		}
		if (i.intValue() < 0) {
			throw new EvaluationException(
					"Could not evaluate BytesPartExpression(" + s + ",\""
							+ this.delimiter + "\"," + i + ")");
		}

		BytesReference column = seek(s, i.intValue(), this.delimiter);
		if (null == column) {
			throw new EvaluationException(
					"Could not evaluate BytesPartExpression(" + s + ",\""
							+ this.delimiter + "\"," + i + ")");
		}

		return new EvaluationResult(column,
				EvaluationResult.ResultType.BYTESREFERENCE);
	}

	private static BytesReference seek(BytesReference s, int index,
			byte[] delimeter) {
		int offset = s.getOffset();
		int length = s.getLength();
		byte[] buffer = s.getReference();
		int lastOffset = offset + length;

		int count = 0;
		int start = -1;

		if (index == 0) {
			start = offset;
		} else {
			for (int i = offset; i < lastOffset; i++) {
				if (delimeter[0] == buffer[i]) {
					count++;
					if (count == index) {
						start = i + 1;
						break;
					}
				}
			}
		}

		if (-1 == start) {
			return null;
		}

		int end = -1;
		for (int i = start; i < lastOffset; i++) {
			if ((delimeter[0] != buffer[i]) || (count != index))
				continue;
			end = i;
			break;
		}

		if (end == -1) {
			end = lastOffset;
		}
		return new BytesReference(buffer, start, end - start);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof BytesPartExpression)))
			return false;
		BytesPartExpression other = (BytesPartExpression) expr;
		return ((this.source == null) && (other.source == null))
				|| ((this.source != null) && (this.source.equals(other.source)) && (((this.delimiter == null) && (other.delimiter == null)) || ((this.delimiter != null)
						&& (Bytes.equals(this.delimiter, other.delimiter)) && (((this.index == null) && (other.index == null)) || ((this.index != null) && (this.index
						.equals(other.index)))))));
	}

	public int hashCode() {
		int result = this.source == null ? 1 : this.source.hashCode();
		result = result * 31
				+ (this.delimiter == null ? 1 : Bytes.hashCode(this.delimiter));
		result = result * 31 + (this.index == null ? 1 : this.index.hashCode());
		return result;
	}

	public String toString() {
		return "bytesPart(" + this.source.toString() + ", "
				+ Bytes.toString(this.delimiter) + ", " + this.index.toString()
				+ ")";
	}

	public void readFields(DataInput in) throws IOException {
		this.source = ((Expression) HbaseObjectWritable.readObject(in, null));
		boolean notNull = in.readBoolean();
		if (notNull)
			this.delimiter = Bytes.readByteArray(in);
		else {
			this.delimiter = null;
		}
		this.index = ((Expression) HbaseObjectWritable.readObject(in, null));
	}

	public void write(DataOutput out) throws IOException {
		HbaseObjectWritable.writeObject(out, this.source,
				this.source.getClass(), null);
		if (this.delimiter == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			Bytes.writeByteArray(out, this.delimiter);
		}
		HbaseObjectWritable.writeObject(out, this.index, this.index.getClass(),
				null);
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.BYTESREFERENCE;
	}
}