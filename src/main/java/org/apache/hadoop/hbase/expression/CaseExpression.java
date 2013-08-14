package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public class CaseExpression implements Expression {
	private Expression conditionExpression;
	private Expression defaultExpression;
	private List<WhenBranch> whenBranches = new ArrayList<WhenBranch>();

	public CaseExpression() {
	}

	public CaseExpression(Expression conditionExpression,
			Expression defaultExpression) {
		this.conditionExpression = conditionExpression;
		this.defaultExpression = defaultExpression;
	}

	public CaseExpression when(Expression matchExpression,
			Expression returnExpression) {
		this.whenBranches
				.add(new WhenBranch(matchExpression, returnExpression));
		return this;
	}

	public Expression getConditionExpression() {
		return this.conditionExpression;
	}

	public Expression getDefaultExpression() {
		return this.defaultExpression;
	}

	public List<WhenBranch> getWhenBranches() {
		return Collections.unmodifiableList(this.whenBranches);
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if ((this.conditionExpression == null)
				|| (this.defaultExpression == null)) {
			throw new EvaluationException("Missing required arguments");
		}
		EvaluationResult cond = this.conditionExpression.evaluate(context);
		for (Iterator<WhenBranch> it = this.whenBranches.iterator(); it
				.hasNext();) {
			WhenBranch when = (WhenBranch) it.next();
			if ((when.matchExpression == null)
					|| (when.returnExpression == null))
				throw new EvaluationException("Missing required arguments");
			EvaluationResult match = when.matchExpression.evaluate(context);
			if (((cond.isNullResult()) && (match.isNullResult()))
					|| (EvaluationResult.compare(cond, match) == 0)) {
				return when.returnExpression.evaluate(context);
			}
		}
		return this.defaultExpression.evaluate(context);
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof CaseExpression)))
			return false;
		CaseExpression other = (CaseExpression) expr;
		if (((this.conditionExpression == null) && (other.conditionExpression != null))
				|| ((this.conditionExpression != null) && (!this.conditionExpression
						.equals(other.conditionExpression)))
				|| ((this.defaultExpression == null) && (other.defaultExpression != null))
				|| ((this.defaultExpression != null) && (!this.defaultExpression
						.equals(other.defaultExpression)))
				|| (this.whenBranches.size() != other.whenBranches.size())) {
			return false;
		}
		Iterator<WhenBranch> it = this.whenBranches.iterator();
		for (Iterator<WhenBranch> itOther = other.whenBranches.iterator(); it
				.hasNext();) {
			WhenBranch when = (WhenBranch) it.next();
			WhenBranch whenOther = (WhenBranch) itOther.next();
			if (!when.equals(whenOther)) {
				return false;
			}
		}
		return true;
	}

	public int hashCode() {
		int result = this.conditionExpression == null ? 1
				: this.conditionExpression.hashCode();
		result = result
				* 31
				+ (this.defaultExpression == null ? 1 : this.defaultExpression
						.hashCode());
		result = result * 31 + this.whenBranches.size();
		for (Iterator<WhenBranch> it = this.whenBranches.iterator(); it
				.hasNext();) {
			result += ((WhenBranch) it.next()).hashCode();
		}
		return result;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("case (");
		sb.append(this.conditionExpression.toString());
		sb.append(")");
		for (Iterator<WhenBranch> it = this.whenBranches.iterator(); it
				.hasNext();) {
			WhenBranch when = (WhenBranch) it.next();
			sb.append(" when (");
			sb.append(when.matchExpression.toString());
			sb.append(") then (");
			sb.append(when.returnExpression.toString());
			sb.append(")");
		}
		sb.append(" else (");
		sb.append(this.defaultExpression.toString());
		sb.append(") end");
		return sb.toString();
	}

	public void readFields(DataInput in) throws IOException {
		this.conditionExpression = ((Expression) HbaseObjectWritable
				.readObject(in, null));
		this.defaultExpression = ((Expression) HbaseObjectWritable.readObject(
				in, null));
		int count = in.readInt();
		for (int i = 0; i < count; i++)
			this.whenBranches.add(new WhenBranch(
					(Expression) HbaseObjectWritable.readObject(in, null),
					(Expression) HbaseObjectWritable.readObject(in, null)));
	}

	public void write(DataOutput out) throws IOException {
		HbaseObjectWritable.writeObject(out, this.conditionExpression,
				this.conditionExpression.getClass(), null);
		HbaseObjectWritable.writeObject(out, this.defaultExpression,
				this.defaultExpression.getClass(), null);
		out.writeInt(this.whenBranches.size());
		for (Iterator<WhenBranch> it = this.whenBranches.iterator(); it
				.hasNext();) {
			WhenBranch when = (WhenBranch) it.next();
			HbaseObjectWritable.writeObject(out, when.matchExpression,
					when.matchExpression.getClass(), null);
			HbaseObjectWritable.writeObject(out, when.returnExpression,
					when.returnExpression.getClass(), null);
		}
	}

	public EvaluationResult.ResultType getReturnType() {
		EvaluationResult.ResultType maxType = this.defaultExpression == null ? EvaluationResult.ResultType.UNKNOWN
				: this.defaultExpression.getReturnType();
		for (Iterator<WhenBranch> it = this.whenBranches.iterator(); it
				.hasNext();) {
			WhenBranch when = (WhenBranch) it.next();
			if (when.returnExpression == null)
				continue;
			EvaluationResult.ResultType whenType = when.returnExpression
					.getReturnType();
			if (maxType == EvaluationResult.ResultType.UNKNOWN)
				maxType = whenType;
			else if (whenType != EvaluationResult.ResultType.UNKNOWN) {
				maxType = EvaluationResult.getMaxResultType(maxType, whenType);
			}
		}
		return maxType;
	}

	public class WhenBranch {
		private Expression matchExpression;
		private Expression returnExpression;

		protected WhenBranch(Expression matchExpression,
				Expression returnExpression) {
			this.matchExpression = matchExpression;
			this.returnExpression = returnExpression;
		}

		public Expression getMatchExpression() {
			return this.matchExpression;
		}

		public Expression getReturnExpression() {
			return this.returnExpression;
		}

		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if ((obj == null) || (!(obj instanceof WhenBranch))) {
				return false;
			}
			WhenBranch other = (WhenBranch) obj;
			return ((this.matchExpression == null) && (other.matchExpression == null))
					|| ((this.matchExpression != null)
							&& (this.matchExpression
									.equals(other.matchExpression)) && (((this.returnExpression == null) && (other.returnExpression == null)) || ((this.returnExpression != null) && (this.returnExpression
							.equals(other.returnExpression)))));
		}

		public int hashCode() {
			int result = this.matchExpression == null ? 1
					: this.matchExpression.hashCode();
			result = result
					* 31
					+ (this.returnExpression == null ? 1
							: this.returnExpression.hashCode());
			return result;
		}
	}
}