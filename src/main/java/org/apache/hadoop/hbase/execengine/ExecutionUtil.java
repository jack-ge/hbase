package org.apache.hadoop.hbase.execengine;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.coprocessor.GroupByCombinedKey;
import org.apache.hadoop.hbase.coprocessor.GroupByIntermediateResult;
import org.apache.hadoop.hbase.coprocessor.batch.BatchExecCall;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.expression.evaluation.StatsValue;
import org.apache.hadoop.hbase.expression.evaluation.TypeConversionException;

import com.google.common.collect.MinMaxPriorityQueue;

public class ExecutionUtil {
	public static MinMaxPriorityQueue<EvaluationResult[]> createPriorityQueue(
			final int[] orderByIndices, final boolean[] ascending, int topCount) {
		return MinMaxPriorityQueue
				.orderedBy(new Comparator<EvaluationResult[]>() {
					public int compare(EvaluationResult[] left,
							EvaluationResult[] right) {
						int comp = left.length - right.length;
						if (comp == 0) {
							for (int i = 0; i < orderByIndices.length; i++) {
								int index = orderByIndices[i];
								comp = EvaluationResult.NULL_AS_MAX_COMPARATOR
										.compare(left[index], right[index]);
								if (comp != 0) {
									if (ascending[i] != false)
										break;
									comp = -comp;
									break;
								}
							}

						}

						return comp;
					}
				}).maximumSize(topCount).create();
	}

	public static class DistinctCallback implements
			BatchExecCall.ServerCallback<Set<GroupByCombinedKey>> {
		private int mapSize = 16;
		private float loadFactor = 0.75F;
		private Set<GroupByCombinedKey> set = null;

		public DistinctCallback() {
		}

		public DistinctCallback(int mapSize, float loadFactor) {
			this.mapSize = mapSize;
			this.loadFactor = loadFactor;
		}

		public synchronized void update(byte[] region,
				Set<GroupByCombinedKey> resultSet) throws IOException {
			for (GroupByCombinedKey value : resultSet)
				this.set.add(value);
		}

		public Set<GroupByCombinedKey> getResult() {
			return this.set;
		}

		public void init() {
			this.set = new HashSet<GroupByCombinedKey>(this.mapSize,
					this.loadFactor);
		}

		public void readFields(DataInput in) throws IOException {
			this.mapSize = in.readInt();
			this.loadFactor = in.readFloat();
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(this.mapSize);
			out.writeFloat(this.loadFactor);
		}
	}

	public static class GroupByCallback
			implements
			BatchExecCall.ServerCallback<Map<GroupByCombinedKey, List<StatsValue>>> {
		private int mapSize = 16;
		private float loadFactor = 0.75F;
		private Map<GroupByCombinedKey, List<StatsValue>> map = null;

		public GroupByCallback() {
		}

		public GroupByCallback(int mapSize, float loadFactor) {
			this.mapSize = mapSize;
			this.loadFactor = loadFactor;
		}

		public synchronized void update(byte[] region,
				Map<GroupByCombinedKey, List<StatsValue>> resultMap)
				throws IOException {
			for (GroupByCombinedKey facet : resultMap.keySet()) {
				List<StatsValue> finalStats = this.map.get(facet);
				Iterator<StatsValue> itFinal;
				Iterator<StatsValue> itTemp;
				if (finalStats != null) {
					itFinal = finalStats.iterator();
					for (itTemp = resultMap.get(facet).iterator(); itFinal
							.hasNext();)
						try {
							((StatsValue) itFinal.next())
									.accumulate((StatsValue) itTemp.next());
						} catch (TypeConversionException e) {
							throw new IOException(e);
						}
				} else {
					this.map.put(facet, resultMap.get(facet));
				}
			}
		}

		public Map<GroupByCombinedKey, List<StatsValue>> getResult() {
			return this.map;
		}

		public void init() {
			this.map = new HashMap<GroupByCombinedKey, List<StatsValue>>(
					this.mapSize, this.loadFactor);
		}

		public void readFields(DataInput in) throws IOException {
			this.mapSize = in.readInt();
			this.loadFactor = in.readFloat();
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(this.mapSize);
			out.writeFloat(this.loadFactor);
		}
	}

	public static class TopNGroupingCallback implements
			BatchExecCall.ServerCallback<GroupByIntermediateResult> {
		private int[] orderByIndices;
		private boolean[] ascending;
		private int topCount;
		private int mapSize = 16;
		private float loadFactor = 0.75F;
		private MinMaxPriorityQueue<EvaluationResult[]> queue = null;
		private Map<GroupByCombinedKey, List<StatsValue>> map = null;

		public TopNGroupingCallback() {
		}

		public TopNGroupingCallback(int[] orderByIndices, boolean[] ascending,
				int topCount, int mapSize, float loadFactor) {
			this.orderByIndices = orderByIndices;
			this.ascending = ascending;
			this.topCount = topCount;
			this.mapSize = mapSize;
			this.loadFactor = loadFactor;
		}

		public synchronized void update(byte[] region,
				GroupByIntermediateResult result) throws IOException {
			Iterator<EvaluationResult[]> iter = result.getTopList().iterator();
			while (iter.hasNext()) {
				this.queue.add(iter.next());
			}
			for (Map.Entry<GroupByCombinedKey, List<StatsValue>> entry : result
					.getStatsMap().entrySet()) {
				List<StatsValue> finalStats = this.map.get(entry.getKey());
				Iterator<StatsValue> itFinal;
				Iterator<StatsValue> itTemp;
				if (finalStats != null) {
					itFinal = finalStats.iterator();
					for (itTemp = entry.getValue().iterator(); itFinal
							.hasNext();)
						try {
							((StatsValue) itFinal.next())
									.accumulate((StatsValue) itTemp.next());
						} catch (TypeConversionException e) {
							throw new IOException(e);
						}
				} else {
					this.map.put(entry.getKey(), entry.getValue());
				}
			}
		}

		public GroupByIntermediateResult getResult() {
			List<EvaluationResult[]> list = new ArrayList<EvaluationResult[]>();

			EvaluationResult[] elem = (EvaluationResult[]) this.queue
					.pollFirst();
			while (elem != null) {
				list.add(elem);
				elem = (EvaluationResult[]) this.queue.pollFirst();
			}

			return new GroupByIntermediateResult(list, this.map);
		}

		public void init() {
			this.queue = ExecutionUtil.createPriorityQueue(this.orderByIndices,
					this.ascending, this.topCount);
			this.map = new HashMap<GroupByCombinedKey, List<StatsValue>>(
					this.mapSize, this.loadFactor);
		}

		public void readFields(DataInput in) throws IOException {
			int count = in.readInt();
			this.orderByIndices = new int[count];
			for (int i = 0; i < count; i++) {
				this.orderByIndices[i] = in.readInt();
			}
			count = in.readInt();
			this.ascending = new boolean[count];
			for (int i = 0; i < count; i++) {
				this.ascending[i] = in.readBoolean();
			}
			this.topCount = in.readInt();
			this.mapSize = in.readInt();
			this.loadFactor = in.readFloat();
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(this.orderByIndices.length);
			for (int i = 0; i < this.orderByIndices.length; i++) {
				out.writeInt(this.orderByIndices[i]);
			}
			out.writeInt(this.ascending.length);
			for (int i = 0; i < this.ascending.length; i++) {
				out.writeBoolean(this.ascending[i]);
			}
			out.writeInt(this.topCount);
			out.writeInt(this.mapSize);
			out.writeFloat(this.loadFactor);
		}
	}

	public static class TopNCallback implements
			BatchExecCall.ServerCallback<List<EvaluationResult[]>> {
		private int[] orderByIndices;
		private boolean[] ascending;
		private int topCount;
		private MinMaxPriorityQueue<EvaluationResult[]> queue = null;

		public TopNCallback() {
		}

		public TopNCallback(int[] orderByIndices, boolean[] ascending,
				int topCount) {
			this.orderByIndices = orderByIndices;
			this.ascending = ascending;
			this.topCount = topCount;
		}

		public void init() {
			this.queue = ExecutionUtil.createPriorityQueue(this.orderByIndices,
					this.ascending, this.topCount);
		}

		public synchronized void update(byte[] region,
				List<EvaluationResult[]> resultList) throws IOException {
			for (Iterator<EvaluationResult[]> iter = resultList.iterator(); iter
					.hasNext();)
				this.queue.add((EvaluationResult[]) iter.next());
		}

		public List<EvaluationResult[]> getResult() {
			List<EvaluationResult[]> ret = new ArrayList<EvaluationResult[]>();

			EvaluationResult[] elem = (EvaluationResult[]) this.queue
					.pollFirst();
			while (elem != null) {
				ret.add(elem);
				elem = (EvaluationResult[]) this.queue.pollFirst();
			}
			return ret;
		}

		public void readFields(DataInput in) throws IOException {
			int count = in.readInt();
			this.orderByIndices = new int[count];
			for (int i = 0; i < count; i++) {
				this.orderByIndices[i] = in.readInt();
			}
			count = in.readInt();
			this.ascending = new boolean[count];
			for (int i = 0; i < count; i++) {
				this.ascending[i] = in.readBoolean();
			}
			this.topCount = in.readInt();
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(this.orderByIndices.length);
			for (int i = 0; i < this.orderByIndices.length; i++) {
				out.writeInt(this.orderByIndices[i]);
			}
			out.writeInt(this.ascending.length);
			for (int i = 0; i < this.ascending.length; i++) {
				out.writeBoolean(this.ascending[i]);
			}
			out.writeInt(this.topCount);
		}
	}
}