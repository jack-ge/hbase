package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.execengine.ExecutionUtil;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.expression.evaluation.StatsValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.google.common.collect.MinMaxPriorityQueue;

public class GroupByImplementation extends BaseEndpointCoprocessor implements
		GroupByProtocol {
	protected static Log log = LogFactory.getLog(GroupByImplementation.class);

	private int MAP_SIZE = 16;
	private float LOAD_FACTOR = 0.75F;

	public Map<GroupByCombinedKey, List<StatsValue>> getStats(Scan scan,
			List<Expression> groupByKeyExpressions,
			List<Expression> groupByStatsExpressions,
			boolean isConstantGroupByKey) throws IOException {
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		Configuration conf = env.getConfiguration();
		InternalScanner scanner = env.getRegion().getScanner(scan);

		List<KeyValue> results = new ArrayList<KeyValue>();
		Map<GroupByCombinedKey, List<StatsValue>> facets = new HashMap<GroupByCombinedKey, List<StatsValue>>(
				isConstantGroupByKey ? 1 : this.MAP_SIZE, this.LOAD_FACTOR);

		int statsCount = groupByStatsExpressions.size();
		EvaluationContext context = new EvaluationContext(conf);
		context.setCurrentRow(results);
		GroupByCombinedKey key = null;
		List<StatsValue> facet = null;
		try {
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(results);

				if (results.isEmpty()) {
					continue;
				}
				key = (isConstantGroupByKey) && (key != null) ? key
						: GroupByCombinedKey.getCombinedKey(
								groupByKeyExpressions, context);

				facet = (isConstantGroupByKey) && (facet != null) ? facet
						: (List<StatsValue>) facets.get(key);

				if (facet == null) {
					facet = new ArrayList<StatsValue>();
					for (int i = 0; i < statsCount; i++) {
						facet.add(new StatsValue());
					}
					facets.put(key, facet);
				}

				Iterator<StatsValue> itValue = facet.iterator();
				Iterator<Expression> itExpr = groupByStatsExpressions
						.iterator();
				while (itExpr.hasNext()) {
					Expression statsExpr = itExpr.next();
					StatsValue value = (StatsValue) itValue.next();
					try {
						value.accumulate(statsExpr.evaluate(context));
					} catch (Throwable t) {
						throw ((IOException) (IOException) new IOException(
								"Exception occurred in evaluation")
								.initCause(t));
					}
				}

				results.clear();
			} while (hasMoreRows);
		} finally {
			scanner.close();
		}

		return facets;
	}

	public List<EvaluationResult[]> getTopNByRow(Scan scan,
			List<Expression> groupByStatsExpressions, int[] orderByIndices,
			boolean[] ascending, int topCount) throws IOException {
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		Configuration conf = env.getConfiguration();
		InternalScanner scanner = env.getRegion().getScanner(scan);

		List<KeyValue> results = new ArrayList<KeyValue>();
		List<EvaluationResult[]> ret = new ArrayList<EvaluationResult[]>();

		MinMaxPriorityQueue<EvaluationResult[]> queue = ExecutionUtil
				.createPriorityQueue(orderByIndices, ascending, topCount);

		int statsCount = groupByStatsExpressions.size();
		EvaluationContext context = new EvaluationContext(conf);
		context.setCurrentRow(results);
		try {
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(results);

				if (results.isEmpty()) {
					continue;
				}
				EvaluationResult[] eval = new EvaluationResult[statsCount];
				for (int i = 0; i < statsCount; i++) {
					eval[i] = (groupByStatsExpressions.get(i))
							.evaluate(context).asSerializableResult();
				}
				queue.add(eval);

				results.clear();
			} while (hasMoreRows);
		} catch (Throwable t) {
			throw ((t instanceof IOException) ? (IOException) t
					: new IOException(t));
		} finally {
			scanner.close();
		}

		EvaluationResult[] elem = (EvaluationResult[]) queue.pollFirst();
		while (elem != null) {
			ret.add(elem);
			elem = (EvaluationResult[]) queue.pollFirst();
		}

		return ret;
	}

	public GroupByIntermediateResult getTopNByRowGrouping(Scan scan,
			List<Expression> groupByKeyExpressions,
			List<Expression> groupByStatsExpressions,
			List<Expression> selectExpressions, Expression havingExpression,
			int[] orderByIndices, boolean[] ascending, int topCount)
			throws IOException {
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		Configuration conf = env.getConfiguration();
		InternalScanner scanner = env.getRegion().getScanner(scan);

		List<KeyValue> results = new ArrayList<KeyValue>();
		MinMaxPriorityQueue<EvaluationResult[]> queue = ExecutionUtil
				.createPriorityQueue(orderByIndices, ascending, topCount);

		GroupByIntermediateResult ret = new GroupByIntermediateResult(null,
				new HashMap<GroupByCombinedKey, List<StatsValue>>(2));

		int statsCount = groupByStatsExpressions.size();
		int selectCount = selectExpressions.size();
		EvaluationContext context = new EvaluationContext(conf);
		context.setCurrentRow(results);
		GroupByCombinedKey lastKey = null;
		List<StatsValue> facet = null;
		try {
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(results);

				if (results.isEmpty()) {
					continue;
				}
				GroupByCombinedKey key = GroupByCombinedKey.getCombinedKey(
						groupByKeyExpressions, context);
				if ((lastKey != null) && (!key.equals(lastKey))) {
					if (ret.getStatsMap().isEmpty()) {
						ret.getStatsMap().put(lastKey, facet);
					} else {
						context.setAggregationValues(lastKey, facet);
						Boolean b = null;
						if (havingExpression != null) {
							b = havingExpression.evaluate(context).asBoolean();
						}
						if ((havingExpression == null)
								|| ((b != null) && (b.booleanValue() == true))) {
							EvaluationResult[] eval = new EvaluationResult[selectCount];
							for (int i = 0; i < eval.length; i++) {
								eval[i] = (selectExpressions.get(i)).evaluate(
										context).asSerializableResult();
							}
							queue.add(eval);
						}
					}
					facet = null;
				}

				if (facet == null) {
					facet = new ArrayList<StatsValue>();
					for (int i = 0; i < statsCount; i++) {
						facet.add(new StatsValue());
					}
				}
				Iterator<StatsValue> itValue = facet.iterator();
				Iterator<Expression> itExpr = groupByStatsExpressions
						.iterator();
				while (itExpr.hasNext()) {
					Expression statsExpr = itExpr.next();
					StatsValue value = (StatsValue) itValue.next();
					value.accumulate(statsExpr.evaluate(context));
				}

				lastKey = key;
				results.clear();
			} while (hasMoreRows);

			if (lastKey != null) {
				ret.getStatsMap().put(lastKey, facet);
			}
			List<EvaluationResult[]> topList = new ArrayList<EvaluationResult[]>();
			EvaluationResult[] elem = (EvaluationResult[]) queue.pollFirst();
			while (elem != null) {
				topList.add(elem);
				elem = (EvaluationResult[]) queue.pollFirst();
			}
			ret.setTopList(topList);
		} catch (Throwable t) {
			throw ((t instanceof IOException) ? (IOException) t
					: new IOException(t));
		} finally {
			scanner.close();
		}

		return ret;
	}

	public Set<GroupByCombinedKey> distinct(Scan scan,
			List<Expression> distinctExpressions) throws IOException {
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		Configuration conf = env.getConfiguration();
		InternalScanner scanner = env.getRegion().getScanner(scan);

		List<KeyValue> results = new ArrayList<KeyValue>();
		Set<GroupByCombinedKey> resultSet = new HashSet<GroupByCombinedKey>(
				this.MAP_SIZE, this.LOAD_FACTOR);

		EvaluationContext context = new EvaluationContext(conf);
		context.setCurrentRow(results);
		try {
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(results);

				if (results.isEmpty()) {
					continue;
				}
				GroupByCombinedKey values = GroupByCombinedKey.getCombinedKey(
						distinctExpressions, context);
				resultSet.add(values);

				results.clear();
			} while (hasMoreRows);
		} finally {
			scanner.close();
		}

		return resultSet;
	}

	public void setMapSize(int mapSize) {
		this.MAP_SIZE = mapSize;
	}

	public void setMapLoadFactor(float loadFactor) {
		this.LOAD_FACTOR = loadFactor;
	}
}