package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.Text;

public class StringMatchExpression implements Expression {
	private Expression matchExpression;
	private String regex;
	private Pattern pattern;

	public StringMatchExpression() {
	}

	public StringMatchExpression(Expression matchExpression, String regex) {
		this.matchExpression = matchExpression;
		this.regex = regex;
		this.pattern = Pattern.compile(regex);
	}

	public Expression getMatchExpression() {
		return this.matchExpression;
	}

	public String getRegex() {
		return this.regex;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if ((this.matchExpression == null) || (this.regex == null)) {
			throw new EvaluationException("Missing required arguments");
		}
		String s = this.matchExpression.evaluate(context).asString();
		if (s == null) {
			return new EvaluationResult();
		}
		return new EvaluationResult(Boolean.valueOf(this.pattern.matcher(s)
				.matches()), EvaluationResult.ResultType.BOOLEAN);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof StringMatchExpression)))
			return false;
		StringMatchExpression other = (StringMatchExpression) expr;
		return ((this.matchExpression == null) && (other.matchExpression == null))
				|| ((this.matchExpression != null)
						&& (this.matchExpression.equals(other.matchExpression)) && (((this.regex == null) && (other.regex == null)) || ((this.regex != null) && (this.regex
						.equals(other.regex)))));
	}

	public int hashCode() {
		int result = this.matchExpression == null ? 1 : this.matchExpression
				.hashCode();
		result = result * 31 + (this.regex == null ? 1 : this.regex.hashCode());
		return result;
	}

	public String toString() {
		return "stringMatch(" + this.matchExpression.toString() + ", "
				+ this.regex + ")";
	}

	public void readFields(DataInput in) throws IOException {
		this.matchExpression = ((Expression) HbaseObjectWritable.readObject(in,
				null));
		boolean notNull = in.readBoolean();
		if (notNull) {
			this.regex = Text.readString(in);
			this.pattern = Pattern.compile(this.regex);
		} else {
			this.regex = null;
			this.pattern = null;
		}
	}

	public void write(DataOutput out) throws IOException {
		HbaseObjectWritable.writeObject(out, this.matchExpression,
				this.matchExpression.getClass(), null);
		if (this.regex == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			Text.writeString(out, this.regex);
		}
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.BOOLEAN;
	}
}