package org.apache.hadoop.hbase.expression.evaluation;

import java.io.Serializable;

public class StatsValue implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Double min;
	private Double max;
	private double sum;
	private double sumOfSquares;
	private long count;
	private long missing;
	private EvaluationResult.ResultType type;

	public StatsValue() {
		reset();
	}

	public void accumulate(EvaluationResult r) throws TypeConversionException {
		if (r.isNullResult()) {
			this.missing += 1L;
		} else if (!r.isNumber()) {
			this.count += 1L;
		} else {
			if ((this.type != EvaluationResult.ResultType.DOUBLE)
					&& (r.getResultType().getCode() > this.type.getCode())) {
				this.type = EvaluationResult.ResultType.DOUBLE;
			}
			double d = r.asDouble().doubleValue();
			double multResult = d * d;
			this.sumOfSquares += multResult;
			this.min = Double.valueOf((this.min == null)
					|| (d < this.min.doubleValue()) ? d : this.min
					.doubleValue());
			this.max = Double.valueOf((this.max == null)
					|| (d > this.max.doubleValue()) ? d : this.max
					.doubleValue());
			this.sum += d;
			this.count += 1L;
		}
	}

	public void accumulate(StatsValue stv) throws TypeConversionException {
		if ((this.type != EvaluationResult.ResultType.DOUBLE)
				&& (stv.getType().getCode() > this.type.getCode())) {
			this.type = EvaluationResult.ResultType.DOUBLE;
		}
		this.min = ((this.min == null)
				|| ((stv.min != null) && (stv.min.doubleValue() < this.min
						.doubleValue())) ? stv.min : this.min);

		this.max = ((this.max == null)
				|| ((stv.max != null) && (stv.max.doubleValue() > this.max
						.doubleValue())) ? stv.max : this.max);

		this.sum += stv.sum;
		this.count += stv.count;
		this.missing += stv.missing;
		this.sumOfSquares += stv.sumOfSquares;
	}

	public void reset() {
		this.min = null;
		this.max = null;
		this.sum = 0.0D;
		this.count = (this.missing = 0L);
		this.sumOfSquares = 0.0D;
		this.type = EvaluationResult.ResultType.LONG;
	}

	public Number getMin() {
		if ((this.min != null)
				&& (this.type.getCode() <= EvaluationResult.ResultType.LONG
						.getCode()))
			return new Long(this.min.longValue());
		return this.min;
	}

	public Number getMax() {
		if ((this.max != null)
				&& (this.type.getCode() <= EvaluationResult.ResultType.LONG
						.getCode()))
			return new Long(this.max.longValue());
		return this.max;
	}

	public Number getSum() {
    if (this.type.getCode() <= EvaluationResult.ResultType.LONG.getCode())
      return new Long((long)this.sum);
    return Double.valueOf(this.sum);
  }

	public Number getSumOfSquares() {
    if (this.type.getCode() <= EvaluationResult.ResultType.LONG.getCode())
      return new Long((long)this.sumOfSquares);
    return Double.valueOf(this.sumOfSquares);
  }

	public long getCount() {
		return this.count;
	}

	public long getMissing() {
		return this.missing;
	}

	public EvaluationResult.ResultType getType() {
		return this.type;
	}
}