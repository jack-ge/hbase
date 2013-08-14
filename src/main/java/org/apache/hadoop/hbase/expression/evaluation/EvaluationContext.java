package org.apache.hadoop.hbase.expression.evaluation;

import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.GroupByCombinedKey;

public class EvaluationContext {
	private CompatibilityMode mode;
	private List<KeyValue> currentRow;
	private GroupByCombinedKey groupByKey;
	private List<StatsValue> groupByStats;

	public EvaluationContext() {
		this(CompatibilityMode.HIVE);
	}

	public EvaluationContext(Configuration conf) {
		this(CompatibilityMode.valueOfWithDefault(
				conf.get("sql.compatibility.mode"), CompatibilityMode.HIVE));
	}

	public EvaluationContext(CompatibilityMode mode) {
		this.mode = mode;
	}

	public void setCurrentRow(List<KeyValue> currentRow) {
		this.currentRow = currentRow;
	}

	public void setAggregationValues(GroupByCombinedKey groupByKey,
			List<StatsValue> groupByStats) {
		this.groupByKey = groupByKey;
		this.groupByStats = groupByStats;
	}

	public void clearAll() {
		this.currentRow = null;
		this.groupByKey = null;
		this.groupByStats = null;
	}

	public CompatibilityMode getCompatibilityMode() {
		return this.mode;
	}

	public BytesReference getRow() {
		if (this.currentRow == null) {
			return null;
		}
		BytesReference found = null;
		Iterator<KeyValue> i$ = this.currentRow.iterator();
		if (i$.hasNext()) {
			KeyValue kv = (KeyValue) i$.next();
			found = new BytesReference(kv.getBuffer(), kv.getRowOffset(),
					kv.getRowLength());
		}

		return found;
	}

	public BytesReference getColumnValue(byte[] family, byte[] qualifier)
			throws EvaluationException {
		if (this.currentRow == null) {
			return null;
		}
		BytesReference found = null;
		for (KeyValue kv : this.currentRow) {
			if (kv.matchingColumn(family, qualifier)) {
				found = new BytesReference(kv.getBuffer(), kv.getValueOffset(),
						kv.getValueLength());

				break;
			}
		}

		return found;
	}

	public EvaluationResult getGroupByKey(int index) throws EvaluationException {
		if (this.groupByKey == null) {
			throw new ValueNotAvailableException(
					"Group-by result not available");
		}
		EvaluationResult[] keys = this.groupByKey.getKeys();

		if ((index < 0) || (index >= keys.length)) {
			throw new ValueNotAvailableException("Invalid group-by key index: "
					+ index);
		}
		return keys[index];
	}

	public StatsValue getGroupByStatsValue(int index)
			throws EvaluationException {
		if (this.groupByStats == null) {
			throw new ValueNotAvailableException(
					"Group-by result not available");
		}
		if ((index < 0) || (index >= this.groupByStats.size())) {
			throw new ValueNotAvailableException(
					"Invalid group-by stats value index: " + index);
		}
		return (StatsValue) this.groupByStats.get(index);
	}

	public static enum CompatibilityMode {
		HIVE, ORACLE, SQLSERVER;

		public static CompatibilityMode valueOfWithDefault(String strValue,
				CompatibilityMode defaultValue) {
			try {
				return strValue == null ? defaultValue : valueOf(strValue
						.toUpperCase());
			} catch (Exception e) {
			}
			return defaultValue;
		}
	}
}