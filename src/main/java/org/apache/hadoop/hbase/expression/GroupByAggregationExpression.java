package org.apache.hadoop.hbase.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.expression.evaluation.StatsValue;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.Text;

public class GroupByAggregationExpression implements Expression {
	public static int INVALID_STATS_INDEX = -1;
	private AggregationType type;
	private Expression subExpression;
	private int statsIndex;

	public GroupByAggregationExpression() {
		this(AggregationType.NO_OP, null);
	}

	public GroupByAggregationExpression(AggregationType type,
			Expression subExpression) {
		this.type = type;
		this.subExpression = subExpression;
		this.statsIndex = INVALID_STATS_INDEX;
	}

	public AggregationType getType() {
		return this.type;
	}

	public Expression getSubExpression() {
		return this.subExpression;
	}

	public int getStatsIndex() {
		return this.statsIndex;
	}

	public void setStatsIndex(int statsIndex) {
		this.statsIndex = statsIndex;
	}

	public EvaluationResult evaluate(EvaluationContext context)
			throws EvaluationException {
		if (this.type == null) {
			throw new EvaluationException("Missing required arguments");
		}
		StatsValue value = context.getGroupByStatsValue(this.statsIndex);
		if (value == null) {
			return new EvaluationResult();
		}
		EvaluationResult res = null;
		switch (this.type.ordinal()) {
		case 1:
			res = new EvaluationResult(value.getSum(), value.getType());
			break;
		case 2:
			res = new EvaluationResult(Double.valueOf(value.getSum()
					.doubleValue() / value.getCount()),
					EvaluationResult.ResultType.DOUBLE);
			break;
		case 3:
			res = new EvaluationResult(Long.valueOf(value.getCount()),
					EvaluationResult.ResultType.LONG);
			break;
		case 4:
			res = new EvaluationResult(value.getMin(), value.getType());
			break;
		case 5:
			res = new EvaluationResult(value.getMax(), value.getType());
			break;
		case 6:
			double avg = value.getSum().doubleValue() / value.getCount();
			double avgOfSumSq = value.getSumOfSquares().doubleValue()
					/ value.getCount();
			res = new EvaluationResult(Double.valueOf(Math.pow(avgOfSumSq - avg
					* avg, 0.5D)), EvaluationResult.ResultType.DOUBLE);
			break;
		default:
			throw new EvaluationException("Unsupported aggregation type: "
					+ this.type);
		}

		return res;
	}

	public boolean equals(Object expr) {
		if (this == expr)
			return true;
		if ((expr == null) || (!(expr instanceof GroupByAggregationExpression)))
			return false;
		GroupByAggregationExpression other = (GroupByAggregationExpression) expr;
		return (this.type == other.type)
				&& (((this.subExpression == null) && (other.subExpression == null)) || ((this.subExpression != null) && (this.subExpression
						.equals(other.subExpression))));
	}

	public int hashCode() {
		int result = this.type == null ? 1 : this.type.hashCode();
		result = result
				* 31
				+ (this.subExpression == null ? 1 : this.subExpression
						.hashCode());
		return result;
	}

	public String toString() {
		return this.type.toString().toLowerCase() + "("
				+ this.subExpression.toString() + ")";
	}

	public void readFields(DataInput in) throws IOException {
		this.type = AggregationType.valueOf(Text.readString(in));
		this.subExpression = ((Expression) HbaseObjectWritable.readObject(in,
				null));
		this.statsIndex = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this.type.toString());
		HbaseObjectWritable.writeObject(out, this.subExpression,
				this.subExpression.getClass(), null);
		out.writeInt(this.statsIndex);
	}

	public EvaluationResult.ResultType getReturnType() {
		return EvaluationResult.ResultType.DOUBLE;
	}

	public static enum AggregationType {
		SUM(1), AVG(2), COUNT(3), MIN(4), MAX(5), STDDEV(6), NO_OP(-1);
		private final int value;

		private AggregationType(int value) {
			this.value=value;
		}

		public int getValue() {
			return value;
		}

	}
}