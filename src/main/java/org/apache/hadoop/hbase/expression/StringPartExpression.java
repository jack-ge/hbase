package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.Text;

public class StringPartExpression implements Expression {
	private Expression source;
	private String delimiter;
	private Expression index;

	public StringPartExpression() {
	}

	public StringPartExpression(Expression source, String delimiter, int index) {
		this(source, delimiter, new ConstantExpression(Integer.valueOf(index)));
	}

	public StringPartExpression(Expression source, String delimiter,
			Expression index) {
		this.source = source;
		this.delimiter = delimiter;
		this.index = index;
	}

	public Expression getSource() {
		return this.source;
	}

	public String getDelimiter() {
		return this.delimiter;
	}

	public Expression getIndex() {
		return this.index;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if ((this.source == null) || (this.delimiter == null)
				|| (this.delimiter.isEmpty()) || (this.index == null)) {
			throw new EvaluationException("Missing required arguments");
		}
		String s = this.source.evaluate(context).asString();
		Integer i = this.index.evaluate(context).asInteger();
		if ((s == null) || (i == null)) {
			return new EvaluationResult();
		}
		if (i.intValue() < 0) {
			throw new EvaluationException(
					"Could not evaluate StringPartExpression(" + s + ",\""
							+ this.delimiter + "\"," + i + ")");
		}

		String result = seek(s, i.intValue(), this.delimiter);

		if (result == null) {
			throw new EvaluationException(
					"Could not evaluate StringPartExpression(" + s + ",\""
							+ this.delimiter + "\"," + i + ")");
		}

		return new EvaluationResult(result, EvaluationResult.ResultType.STRING);
	}

	private static String seek(String s, int index, String seperator) {
		int offset = 0;
		int length = s.length();

		int count = 0;
		int start = -1;
		char delimeter = seperator.charAt(0);

		if (index == 0) {
			start = 0;
		} else {
			for (int i = offset; i < length; i++) {
				if (delimeter == s.charAt(i)) {
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
		for (int i = start; i < length; i++) {
			if ((delimeter != s.charAt(i)) || (count != index))
				continue;
			end = i;
			break;
		}

		if (end == -1) {
			end = length;
		}

		return s.substring(start, end);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof StringPartExpression)))
			return false;
		StringPartExpression other = (StringPartExpression) expr;
		return ((this.source == null) && (other.source == null))
				|| ((this.source != null) && (this.source.equals(other.source)) && (((this.delimiter == null) && (other.delimiter == null)) || ((this.delimiter != null)
						&& (this.delimiter.equals(other.delimiter)) && (((this.index == null) && (other.index == null)) || ((this.index != null) && (this.index
						.equals(other.index)))))));
	}

	public int hashCode() {
		int result = this.source == null ? 1 : this.source.hashCode();
		result = result * 31
				+ (this.delimiter == null ? 1 : this.delimiter.hashCode());
		result = result * 31 + (this.index == null ? 1 : this.index.hashCode());
		return result;
	}

	public String toString() {
		return "stringPart(" + this.source.toString() + ", " + this.delimiter
				+ ", " + this.index.toString() + ")";
	}

	public void readFields(DataInput in) throws IOException {
		this.source = ((Expression) HbaseObjectWritable.readObject(in, null));
		boolean notNull = in.readBoolean();
		if (notNull)
			this.delimiter = Text.readString(in);
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
			Text.writeString(out, this.delimiter);
		}
		HbaseObjectWritable.writeObject(out, this.index, this.index.getClass(),
				null);
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.STRING;
	}
}