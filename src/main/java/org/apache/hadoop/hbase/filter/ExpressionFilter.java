package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public class ExpressionFilter extends FilterBase {
	protected Expression expression;
	protected EvaluationContext context;
	protected boolean filtered;

	public ExpressionFilter() {
		this(null);
	}

	public ExpressionFilter(Expression expression) {
		this.expression = expression;
		this.context = new EvaluationContext(HBaseConfiguration.create());
		this.filtered = false;
	}

	public void reset() {
		this.filtered = false;
	}

	public void filterRow(List<KeyValue> kvs) {
		if (this.expression != null) {
			this.context.setCurrentRow(kvs);
			try {
				EvaluationResult r = this.expression.evaluate(this.context);
				if ((!r.isNullResult()) && (r.asBoolean().booleanValue())) {
					this.filtered = false;
					return;
				}
			} catch (Throwable t) {
			}
		}
		this.filtered = true;
	}

	public boolean hasFilterRow() {
		return true;
	}

	public boolean filterRow() {
		return this.filtered;
	}

	public void readFields(DataInput in) throws IOException {
		this.expression = ((Expression) HbaseObjectWritable
				.readObject(in, null));
		this.context.clearAll();
		reset();
	}

	public void write(DataOutput out) throws IOException {
		HbaseObjectWritable.writeObject(out, this.expression,
				this.expression.getClass(), null);
	}
}