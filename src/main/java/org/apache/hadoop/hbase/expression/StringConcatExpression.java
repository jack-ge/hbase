package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public class StringConcatExpression implements Expression {
	private Expression[] parts;

	public StringConcatExpression() {
	}

	public StringConcatExpression(Expression[] parts) {
		this.parts = parts;
	}

	public StringConcatExpression(List<Expression> parts) {
		if (parts != null) {
			this.parts = new Expression[parts.size()];
			for (int i = 0; i < parts.size(); i++)
				this.parts[i] = ((Expression) parts.get(i));
		}
	}

	public Expression[] getParts() {
		return this.parts;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if (this.parts == null) {
			throw new EvaluationException("Missing required arguments");
		}
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < this.parts.length; i++) {
			if (this.parts[i] == null) {
				throw new EvaluationException("Missing required arguments");
			}
			String s = this.parts[i].evaluate(context).asString();
			sb.append(s);
		}

		return new EvaluationResult(sb.toString(),
				EvaluationResult.ResultType.STRING);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof StringConcatExpression)))
			return false;
		StringConcatExpression other = (StringConcatExpression) expr;
		if ((this.parts == null) && (other.parts == null))
			return true;
		if (((this.parts == null) && (other.parts != null))
				|| ((this.parts != null) && (other.parts == null))
				|| (this.parts.length != other.parts.length)) {
			return false;
		}
		for (int i = 0; i < this.parts.length; i++) {
			if (!this.parts[i].equals(other.parts[i]))
				return false;
		}
		return true;
	}

	public int hashCode() {
		if (this.parts == null)
			return 1;
		int result = 11;
		for (int i = 0; i < this.parts.length; i++) {
			result = result * 31
					+ (this.parts[i] == null ? 1 : this.parts[i].hashCode());
		}
		return result;
	}

	public String toString() {
		String res = "stringConcat(";
		for (int i = 0; i < this.parts.length; i++) {
			res = new StringBuilder().append(res).append(i > 0 ? ", " : "")
					.append(this.parts[i].toString()).toString();
		}
		return new StringBuilder().append(res).append(")").toString();
	}

	public void readFields(DataInput in) throws IOException {
		int count = in.readInt();
		if (count < 0) {
			this.parts = null;
			return;
		}
		this.parts = new Expression[count];
		for (int i = 0; i < count; i++)
			this.parts[i] = ((Expression) HbaseObjectWritable.readObject(in,
					null));
	}

	public void write(DataOutput out) throws IOException {
		if (this.parts == null) {
			out.writeInt(-1);
			return;
		}
		out.writeInt(this.parts.length);
		for (int i = 0; i < this.parts.length; i++)
			HbaseObjectWritable.writeObject(out, this.parts[i],
					this.parts[i].getClass(), null);
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.STRING;
	}
}