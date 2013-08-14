package org.apache.hadoop.hbase.coprocessor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.io.Writable;

public class GroupByCombinedKey implements Writable, Serializable {
	private static final long serialVersionUID = 2560573700294001437L;
	private EvaluationResult[] keys;
	public static final Comparator<GroupByCombinedKey> DEFAULT_COMPARATOR = new Comparator<GroupByCombinedKey>() {
		public int compare(GroupByCombinedKey left, GroupByCombinedKey right) {
			int comp = left.keys.length - right.keys.length;
			if (comp != 0) {
				return comp;
			}
			for (int i = 0; i < left.keys.length; i++) {
				comp = EvaluationResult.NULL_AS_MAX_COMPARATOR.compare(
						left.keys[i], right.keys[i]);
				if (comp != 0) {
					return comp;
				}
			}
			return 0;
		}
	};

	public GroupByCombinedKey() {
		this.keys = new EvaluationResult[0];
	}

	protected GroupByCombinedKey(EvaluationResult[] keys) {
		this();
		if (keys != null)
			this.keys = keys;
	}

	public static GroupByCombinedKey getCombinedKey(
			List<Expression> groupByKeyExpressions, EvaluationContext context)
			throws IOException {
		int size = groupByKeyExpressions.size();
		EvaluationResult[] keys = new EvaluationResult[size];
		try {
			for (int i = 0; i < size; i++)
				keys[i] = ((Expression) groupByKeyExpressions.get(i)).evaluate(
						context).asSerializableResult();
		} catch (Throwable t) {
			throw ((IOException) (IOException) new IOException().initCause(t));
		}
		return new GroupByCombinedKey(keys);
	}

	public EvaluationResult[] getKeys() {
		return this.keys;
	}

	public String getStringValue(String delimiter) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < this.keys.length; i++) {
			sb.append(this.keys[i].toString());
			if (i < this.keys.length - 1) {
				sb.append(delimiter);
			}
		}
		return sb.toString();
	}

	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof GroupByCombinedKey)) {
			return false;
		}
		return DEFAULT_COMPARATOR.compare(this, (GroupByCombinedKey) obj) == 0;
	}

	public int hashCode() {
		if (this.keys == null) {
			return super.hashCode();
		}
		int result = 11;
		for (int i = 0; i < this.keys.length; i++) {
			result = 37 * result + this.keys[i].hashCode();
		}

		return result;
	}

	public void readFields(DataInput in) throws IOException {
		int count = in.readInt();
		this.keys = new EvaluationResult[count];
		for (int i = 0; i < count; i++) {
			EvaluationResult key = new EvaluationResult();
			key.readFields(in);
			this.keys[i] = key;
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.keys.length);
		for (EvaluationResult key : this.keys)
			key.write(out);
	}
}