package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.coprocessor.GroupByCombinedKey;
import org.apache.hadoop.hbase.coprocessor.GroupByIntermediateResult;
import org.apache.hadoop.hbase.coprocessor.GroupByProtocol;
import org.apache.hadoop.hbase.execengine.ExecutionClient;
import org.apache.hadoop.hbase.expression.BytesPartExpression;
import org.apache.hadoop.hbase.expression.ColumnValueExpression;
import org.apache.hadoop.hbase.expression.ConstantExpression;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.ExpressionException;
import org.apache.hadoop.hbase.expression.GroupByAggregationExpression;
import org.apache.hadoop.hbase.expression.GroupByKeyExpression;
import org.apache.hadoop.hbase.expression.RowExpression;
import org.apache.hadoop.hbase.expression.SubSequenceExpression;
import org.apache.hadoop.hbase.expression.SubstringExpression;
import org.apache.hadoop.hbase.expression.ToBytesExpression;
import org.apache.hadoop.hbase.expression.ToStringExpression;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.expression.evaluation.StatsValue;
import org.apache.hadoop.hbase.expression.evaluation.TypeConversionException;
import org.apache.hadoop.hbase.expression.visitor.ColumnVisitor;
import org.apache.hadoop.hbase.expression.visitor.ExpressionTraversal;
import org.apache.hadoop.hbase.expression.visitor.ExpressionVisitor;
import org.apache.hadoop.hbase.expression.visitor.IsConstantVisitor;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.MinMaxPriorityQueue;

public class GroupByClient {
	Configuration conf;
	private int MAP_SIZE = 16;
	private float LOAD_FACTOR = 0.75F;
	private static final String BATCH_EXEC = "hbase.groupby.batch.execution";
	protected static Log log = LogFactory.getLog(GroupByClient.class);

	public GroupByClient(Configuration conf) {
		this.conf = conf;
	}

	public List<EvaluationResult[]> groupBy(byte[] tableName, Scan[] scans,
			List<Expression> groupByKeyExpressions,
			List<Expression> selectExpressions, Expression havingExpression)
			throws Throwable {
		return groupBy(tableName, scans, groupByKeyExpressions,
				selectExpressions, havingExpression, null, null);
	}

	public List<EvaluationResult[]> groupBy(byte[] tableName, Scan[] scans,
			List<Expression> groupByKeyExpressions,
			List<Expression> selectExpressions, Expression havingExpression,
			int[] orderByIndices, boolean[] ascending) throws Throwable {
		if (this.conf.getBoolean(BATCH_EXEC, false)) {
			return new ExecutionClient(this.conf).groupBy(tableName, scans,
					groupByKeyExpressions, selectExpressions, havingExpression,
					orderByIndices, ascending);
		}
		List<Expression> groupByStatsExpressions = processSelectExpressions(
				groupByKeyExpressions, selectExpressions, havingExpression);

		Comparator<GroupByCombinedKey> comparator = processSortSpecs(
				selectExpressions, orderByIndices, ascending);

		List<EvaluationResult[]> ret = new ArrayList<EvaluationResult[]>();
		Map<GroupByCombinedKey, List<StatsValue>> groupByMap = getStatsInternal(
				tableName, scans, groupByKeyExpressions,
				groupByStatsExpressions, comparator != null, comparator);

		EvaluationContext context = new EvaluationContext(this.conf);
		int count = selectExpressions.size();

		for (Map.Entry<GroupByCombinedKey, List<StatsValue>> entry : groupByMap
				.entrySet()) {
			context.setAggregationValues(entry.getKey(), entry.getValue());
			Boolean b = null;
			if (havingExpression != null) {
				b = havingExpression.evaluate(context).asBoolean();
			}
			if ((havingExpression == null)
					|| ((b != null) && (b.booleanValue() == true))) {
				EvaluationResult[] resultRow = new EvaluationResult[count];
				for (int i = 0; i < count; i++) {
					resultRow[i] = selectExpressions.get(i).evaluate(context);
				}
				ret.add(resultRow);
			}
		}

		if (comparator == null) {
			log.debug("Perform additional sorting on resultsets... ");
			Collections.sort(ret, createComparator(orderByIndices, ascending));
		}

		return ret;
	}

	public List<EvaluationResult[]> getTopNByRow(byte[] tableName,
			final Scan[] scans, final List<Expression> selectExpressions,
			final int[] orderByIndices, final boolean[] ascending,
			final int topCount) throws Throwable {
		if (this.conf.getBoolean(BATCH_EXEC, false)) {
			return new ExecutionClient(this.conf).getTopNByRow(tableName,
					scans, selectExpressions, orderByIndices, ascending,
					topCount);
		}
		final MinMaxPriorityQueue<EvaluationResult[]> queue = MinMaxPriorityQueue
				.orderedBy(createComparator(orderByIndices, ascending))
				.maximumSize(topCount).create();

		validateAndDecorateParameters(scans, new ArrayList<Expression>(),
				selectExpressions);
		Callback<List<EvaluationResult[]>> topNCB = new Batch.Callback<List<EvaluationResult[]>>() {
			public synchronized void update(byte[] region, byte[] row,
					List<EvaluationResult[]> resultList) {
				for (Iterator<EvaluationResult[]> iter = resultList.iterator(); iter
						.hasNext();)
					queue.add(iter.next());
			}
		};
		HTable table = new HTable(this.conf, tableName);

		List<Pair<Pair<byte[], byte[]>, Batch.Call<GroupByProtocol, List<EvaluationResult[]>>>> calls = new ArrayList<Pair<Pair<byte[], byte[]>, Batch.Call<GroupByProtocol, List<EvaluationResult[]>>>>();

		for (final Scan scan : scans) {
			calls.add(new Pair<Pair<byte[], byte[]>, Batch.Call<GroupByProtocol, List<EvaluationResult[]>>>(
					new Pair<byte[], byte[]>(scan.getStartRow(), scan
							.getStopRow()),
					new Batch.Call<GroupByProtocol, List<EvaluationResult[]>>() {
						public List<EvaluationResult[]> call(
								GroupByProtocol instance) throws IOException {
							return instance.getTopNByRow(scan,
									selectExpressions, orderByIndices,
									ascending, topCount);
						}
					}));
		}
		try {
			table.coprocessorExec(GroupByProtocol.class, calls, topNCB);
		} finally {
			table.close();
		}

		List<EvaluationResult[]> ret = new ArrayList<EvaluationResult[]>();
		EvaluationResult[] elem = queue.pollFirst();
		while (elem != null) {
			ret.add(elem);
			elem = queue.pollFirst();
		}
		return ret;
	}

	public List<EvaluationResult[]> getTopN(byte[] tableName, Scan[] scans,
			List<Expression> groupByKeyExpressions,
			List<Expression> selectExpressions, Expression havingExpression,
			int[] orderByIndices, boolean[] ascending, int topCount)
			throws Throwable {
		if (this.conf.getBoolean(BATCH_EXEC, false)) {
			return new ExecutionClient(this.conf).getTopN(tableName, scans,
					groupByKeyExpressions, selectExpressions, havingExpression,
					orderByIndices, ascending, topCount);
		}
		boolean isRowGrouping = isRowGrouping(groupByKeyExpressions);
		log.debug("Is row grouping? " + isRowGrouping);
		List<EvaluationResult[]> ret;
		if (isRowGrouping) {
			List<Expression> groupByStatsExpressions = processSelectExpressions(
					groupByKeyExpressions, selectExpressions, havingExpression);

			Comparator<GroupByCombinedKey> comparator = processSortSpecs(
					selectExpressions, orderByIndices, ascending);

			boolean isSortedMap = comparator != null;
			MinMaxPriorityQueue<EvaluationResult[]> queue = MinMaxPriorityQueue
					.orderedBy(createComparator(orderByIndices, ascending))
					.maximumSize(topCount).create();

			Map<GroupByCombinedKey, List<StatsValue>> map = getTopNInternal(
					tableName, scans, groupByKeyExpressions,
					groupByStatsExpressions, selectExpressions,
					havingExpression, orderByIndices, ascending, topCount,
					queue, isSortedMap, comparator);

			EvaluationContext context = new EvaluationContext(this.conf);
			int count = selectExpressions.size();
			int counter = 0;
			for (Map.Entry<GroupByCombinedKey, List<StatsValue>> entry : map
					.entrySet()) {
				if (isSortedMap) {
					counter++;
					if (counter > topCount)
						break;
				}
				context.setAggregationValues(
						(GroupByCombinedKey) entry.getKey(),
						(List<StatsValue>) entry.getValue());
				Boolean b = null;
				if (havingExpression != null) {
					b = havingExpression.evaluate(context).asBoolean();
				}
				if ((havingExpression == null)
						|| ((b != null) && (b.booleanValue() == true))) {
					EvaluationResult[] resultRow = new EvaluationResult[count];
					for (int i = 0; i < count; i++) {
						resultRow[i] = ((Expression) selectExpressions.get(i))
								.evaluate(context);
					}
					queue.add(resultRow);
				}
			}

			ret = new ArrayList<EvaluationResult[]>();
			EvaluationResult[] elem = (EvaluationResult[]) queue.pollFirst();
			while (elem != null) {
				ret.add(elem);
				elem = (EvaluationResult[]) queue.pollFirst();
			}
		} else {
			List<EvaluationResult[]> groupByResults = groupBy(tableName, scans,
					groupByKeyExpressions, selectExpressions, havingExpression,
					orderByIndices, ascending);

			ret = groupByResults.size() <= topCount ? groupByResults
					: groupByResults.subList(0, topCount);
		}

		return ret;
	}

	public List<EvaluationResult[]> distinct(byte[] tableName, Scan[] scans,
			final List<Expression> distinctExpressions,
			GroupByAggregationExpression.AggregationType type) throws Throwable {
		if (this.conf.getBoolean(BATCH_EXEC, false)) {
			return new ExecutionClient(this.conf).distinct(tableName, scans,
					distinctExpressions, type);
		}
		if ((type != null)
				&& (type != GroupByAggregationExpression.AggregationType.COUNT)
				&& (distinctExpressions.size() > 1)) {
			throw new IOException("Aggregation '" + type.toString()
					+ "' is not allowed on non-numeric types");
		}
		validateAndDecorateParameters(scans, distinctExpressions, null);
		final SortedSet<GroupByCombinedKey> finalSet = new TreeSet<GroupByCombinedKey>(
				GroupByCombinedKey.DEFAULT_COMPARATOR);

		Callback<Set<GroupByCombinedKey>> distinctCB = new Batch.Callback<Set<GroupByCombinedKey>>() {
			public synchronized void update(byte[] region, byte[] row,
					Set<GroupByCombinedKey> resultSet) {
				for (GroupByCombinedKey value : resultSet)
					finalSet.add(value);
			}
		};
		HTable table = new HTable(this.conf, tableName);

		List<Pair<Pair<byte[], byte[]>, Batch.Call<GroupByProtocol, Set<GroupByCombinedKey>>>> calls = new ArrayList<Pair<Pair<byte[], byte[]>, Batch.Call<GroupByProtocol, Set<GroupByCombinedKey>>>>();

		for (final Scan scan : scans)
			calls.add(new Pair<Pair<byte[], byte[]>, Batch.Call<GroupByProtocol, Set<GroupByCombinedKey>>>(
					new Pair<byte[], byte[]>(scan.getStartRow(), scan
							.getStopRow()),
					new Batch.Call<GroupByProtocol, Set<GroupByCombinedKey>>() {
						public Set<GroupByCombinedKey> call(
								GroupByProtocol instance) throws IOException {
							instance.setMapSize(GroupByClient.this.getMapSize());
							return instance.distinct(scan, distinctExpressions);
						}
					}));
		try {
			table.coprocessorExec(GroupByProtocol.class, calls, distinctCB);
		} finally {
			table.close();
		}

		List<EvaluationResult[]> res = new ArrayList<EvaluationResult[]>();
		if (type == null) {
			for (GroupByCombinedKey value : finalSet)
				res.add(value.getKeys());
		} else {
			EvaluationResult eval = null;
			switch (type.ordinal()) {
			case 1:
				eval = new EvaluationResult(Integer.valueOf(finalSet.size()),
						EvaluationResult.ResultType.INTEGER);
				break;
			case 2:
				eval = finalSet.isEmpty() ? new EvaluationResult()
						: ((GroupByCombinedKey) finalSet.first()).getKeys()[0];
				break;
			case 3:
				eval = finalSet.isEmpty() ? new EvaluationResult()
						: ((GroupByCombinedKey) finalSet.last()).getKeys()[0];
				break;
			case 4:
				if (!finalSet.isEmpty()) {
					eval = new EvaluationResult(Integer.valueOf(0),
							EvaluationResult.ResultType.LONG);
					for (GroupByCombinedKey value : finalSet) {
						EvaluationResult val = value.getKeys()[0];
						if (!val.isNullResult())
							eval = EvaluationResult.numberAdd(eval, val);
					}
				} else {
					eval = new EvaluationResult();
				}
				break;
			case 5:
				int count = finalSet.size();
				if (count > 0) {
					eval = new EvaluationResult(Integer.valueOf(0),
							EvaluationResult.ResultType.LONG);
					for (GroupByCombinedKey value : finalSet) {
						EvaluationResult val = value.getKeys()[0];
						if (!val.isNullResult())
							eval = EvaluationResult.numberAdd(eval, val);
					}
					eval = EvaluationResult.numberDivide(eval,
							new EvaluationResult(Integer.valueOf(count),
									EvaluationResult.ResultType.INTEGER));
				} else {
					eval = new EvaluationResult();
				}
				break;
			case 6:
				count = finalSet.size();
				if (count > 0) {
					double sum = 0.0D;
					double sumOfSquares = 0.0D;
					for (GroupByCombinedKey value : finalSet) {
						EvaluationResult val = value.getKeys()[0];
						if (!val.isNullResult()) {
							double d = val.asDouble().doubleValue();
							sum += d;
							sumOfSquares += d * d;
						}
					}
					double avg = sum / count;
					double avgOfSumOfSquares = sumOfSquares / count;
					eval = new EvaluationResult(Double.valueOf(Math.pow(
							avgOfSumOfSquares - avg * avg, 0.5D)),
							EvaluationResult.ResultType.DOUBLE);
				} else {
					eval = new EvaluationResult();
				}
				break;
			default:
				throw new IOException("Unsupported aggregation type " + type);
			}

			res.add(new EvaluationResult[] { eval });
		}

		return res;
	}

	protected boolean isRowGrouping(List<Expression> keyExpressions) {
		if (keyExpressions.size() != 1) {
			return false;
		}
		Expression keyExpr = (Expression) keyExpressions.get(0);
		if ((keyExpr instanceof SubSequenceExpression)) {
			return ((((SubSequenceExpression) keyExpr).getSource() instanceof RowExpression))
					&& ((((SubSequenceExpression) keyExpr).getStart() instanceof ConstantExpression))
					&& (((ConstantExpression) ((SubSequenceExpression) keyExpr)
							.getStart()).getConstant().equals(Integer
							.valueOf(0)));
		}

		if ((keyExpr instanceof ToBytesExpression)) {
			Expression firstLevelExpr = ((ToBytesExpression) keyExpr)
					.getSubExpression();
			if ((firstLevelExpr instanceof SubstringExpression)) {
				return ((((SubstringExpression) firstLevelExpr).getSource() instanceof ToStringExpression))
						&& ((((ToStringExpression) ((SubstringExpression) firstLevelExpr)
								.getSource()).getSubExpression() instanceof RowExpression))
						&& ((((SubstringExpression) firstLevelExpr).getStart() instanceof ConstantExpression))
						&& (((ConstantExpression) ((SubstringExpression) firstLevelExpr)
								.getStart()).getConstant().equals(Integer
								.valueOf(0)));
			}

			if ((firstLevelExpr instanceof BytesPartExpression)) {
				return ((((BytesPartExpression) firstLevelExpr).getSource() instanceof RowExpression))
						&& ((((BytesPartExpression) firstLevelExpr).getIndex() instanceof ConstantExpression))
						&& (((ConstantExpression) ((BytesPartExpression) firstLevelExpr)
								.getIndex()).getConstant().equals(Integer
								.valueOf(0)));
			}

			return false;
		}

		return (keyExpr instanceof RowExpression);
	}

	protected Map<GroupByCombinedKey, List<StatsValue>> getTopNInternal(
			byte[] tableName, Scan[] scans,
			final List<Expression> groupByKeyExpressions,
			final List<Expression> groupByStatsExpressions,
			final List<Expression> selectExpressions,
			final Expression havingExpression, final int[] orderByIndices,
			final boolean[] ascending, final int topCount,
			final MinMaxPriorityQueue<EvaluationResult[]> queue,
			boolean requireSortedMap, Comparator<GroupByCombinedKey> comparator)
			throws Throwable {
		final Map<GroupByCombinedKey, List<StatsValue>> map = requireSortedMap ? new TreeMap<GroupByCombinedKey, List<StatsValue>>(
				comparator)
				: new HashMap<GroupByCombinedKey, List<StatsValue>>();

		validateAndDecorateParameters(scans, groupByKeyExpressions,
				groupByStatsExpressions);
		class TopNCallback implements Batch.Callback<GroupByIntermediateResult> {
			Throwable lastException = null;

			public Throwable getLastException() {
				return this.lastException;
			}

			public synchronized void update(byte[] region, byte[] row,
					GroupByIntermediateResult result) {
				Iterator<EvaluationResult[]> iter = result.getTopList()
						.iterator();
				while (iter.hasNext()) {
					queue.add((EvaluationResult[]) iter.next());
				}
				for (Map.Entry<GroupByCombinedKey, List<StatsValue>> entry : result
						.getStatsMap().entrySet()) {
					List<StatsValue> finalStats = map.get(entry.getKey());
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
								this.lastException = e;
							}
					} else {
						map.put(entry.getKey(), entry.getValue());
					}
				}
			}
		}
		;

		TopNCallback topNCB = new TopNCallback();
		HTable table = new HTable(this.conf, tableName);

		List<Pair<Pair<byte[], byte[]>, Batch.Call<GroupByProtocol, GroupByIntermediateResult>>> calls = new ArrayList<Pair<Pair<byte[], byte[]>, Batch.Call<GroupByProtocol, GroupByIntermediateResult>>>();

		for (final Scan scan : scans) {
			calls.add(new Pair<Pair<byte[], byte[]>, Batch.Call<GroupByProtocol, GroupByIntermediateResult>>(
					new Pair<byte[], byte[]>(scan.getStartRow(), scan
							.getStopRow()),
					new Batch.Call<GroupByProtocol, GroupByIntermediateResult>() {
						public GroupByIntermediateResult call(
								GroupByProtocol instance) throws IOException {
							return instance.getTopNByRowGrouping(scan,
									groupByKeyExpressions,
									groupByStatsExpressions, selectExpressions,
									havingExpression, orderByIndices,
									ascending, topCount);
						}
					}));
		}
		try {
			table.coprocessorExec(GroupByProtocol.class, calls, topNCB);
		} finally {
			table.close();
		}

		if (topNCB.getLastException() != null) {
			throw topNCB.getLastException();
		}

		return map;
	}

	protected Map<GroupByCombinedKey, List<StatsValue>> getStatsInternal(
			byte[] tableName, Scan[] scans,
			final List<Expression> groupByKeyExpressions,
			final List<Expression> groupByStatsExpressions,
			final boolean requireSortedMap,
			final Comparator<GroupByCombinedKey> comparator) throws Throwable {
		final boolean isConstantGroupByKey = validateAndDecorateParameters(
				scans, groupByKeyExpressions, groupByStatsExpressions);

		log.debug("Is constant group-by key? " + isConstantGroupByKey);

		class RowNumCallback implements
				Batch.Callback<Map<GroupByCombinedKey, List<StatsValue>>> {
			private Map<GroupByCombinedKey, List<StatsValue>> finalMap = requireSortedMap ? new TreeMap<GroupByCombinedKey, List<StatsValue>>(
					comparator)
					: new HashMap<GroupByCombinedKey, List<StatsValue>>();

			Throwable lastException = null;

			public Map<GroupByCombinedKey, List<StatsValue>> getFinalMap() {
				return this.finalMap;
			}

			public Throwable getLastException() {
				return this.lastException;
			}

			public synchronized void update(byte[] region, byte[] row,
					Map<GroupByCombinedKey, List<StatsValue>> resultMap) {
				for (GroupByCombinedKey facet : resultMap.keySet()) {
					List<StatsValue> finalStats = this.finalMap.get(facet);
					Iterator<StatsValue> itFinal;
					Iterator<StatsValue> itTemp;
					if (finalStats != null) {
						itFinal = finalStats.iterator();
						for (itTemp = resultMap.get(facet).iterator(); itFinal
								.hasNext();)
							try {
								itFinal.next().accumulate(
										(StatsValue) itTemp.next());
							} catch (TypeConversionException e) {
								this.lastException = e;
							}
					} else {
						this.finalMap.put(facet, resultMap.get(facet));
					}
				}
			}
		}
		;

		RowNumCallback rowNumCB = new RowNumCallback();
		HTable table = new HTable(this.conf, tableName);

		List<Pair<Pair<byte[], byte[]>, Batch.Call<GroupByProtocol, Map<GroupByCombinedKey, List<StatsValue>>>>> calls = new ArrayList<Pair<Pair<byte[], byte[]>, Batch.Call<GroupByProtocol, Map<GroupByCombinedKey, List<StatsValue>>>>>();

		for (final Scan scan : scans) {
			calls.add(new Pair<Pair<byte[], byte[]>, Batch.Call<GroupByProtocol, Map<GroupByCombinedKey, List<StatsValue>>>>(
					new Pair<byte[], byte[]>(scan.getStartRow(), scan
							.getStopRow()),
					new Batch.Call<GroupByProtocol, Map<GroupByCombinedKey, List<StatsValue>>>() {
						public Map<GroupByCombinedKey, List<StatsValue>> call(
								GroupByProtocol instance) throws IOException {
							instance.setMapSize(GroupByClient.this.getMapSize());
							return instance.getStats(scan,
									groupByKeyExpressions,
									groupByStatsExpressions,
									isConstantGroupByKey);
						}
					}));
		}
		try {
			table.coprocessorExec(GroupByProtocol.class, calls, rowNumCB);
		} finally {
			table.close();
		}

		if (rowNumCB.getLastException() != null) {
			throw rowNumCB.getLastException();
		}

		return rowNumCB.getFinalMap();
	}

	private List<Expression> processSelectExpressions(
			final List<Expression> groupByKeyExpressions,
			List<Expression> selectExpressions, Expression havingExpression)
			throws ExpressionException {
		final List<Expression> groupByStatsExpressions = new ArrayList<Expression>();
		ExpressionVisitor visitor = new ExpressionVisitor() {
			public ExpressionVisitor.ReturnCode processExpression(
					Expression expression) throws ExpressionException {
				if ((expression instanceof RowExpression)) {
					throw new ExpressionException(
							"The group-by select expression cannot contain RowExpression");
				}
				if ((expression instanceof ColumnValueExpression)) {
					throw new ExpressionException(
							"The group-by select expression cannot contain ColumnValueExpression");
				}
				if ((expression instanceof GroupByKeyExpression)) {
					GroupByKeyExpression keyExpr = (GroupByKeyExpression) expression;
					Expression refExpr = keyExpr.getReferenceExpression();
					if (refExpr == null) {
						throw new ExpressionException(
								"The group-by key expression cannot contain null expressions");
					}
					for (int i = 0; i < groupByKeyExpressions.size(); i++) {
						if (refExpr.equals(groupByKeyExpressions.get(i))) {
							keyExpr.setKeyIndex(i);
							break;
						}
					}
					if (keyExpr.getKeyIndex() == GroupByKeyExpression.INVALID_KEY_ID) {
						throw new ExpressionException(
								"Could not find group-by key expression: "
										+ refExpr);
					}
					return ExpressionVisitor.ReturnCode.SKIP_SUBTREE;
				}
				if ((expression instanceof GroupByAggregationExpression)) {
					GroupByAggregationExpression aggrExpr = (GroupByAggregationExpression) expression;
					Expression statsExpr = aggrExpr.getSubExpression();
					if (statsExpr == null) {
						throw new ExpressionException(
								"The group-by aggregation expression cannot contain null expressions");
					}
					if ((aggrExpr.getType() != GroupByAggregationExpression.AggregationType.COUNT)
							&& (!EvaluationResult.isNumber(statsExpr
									.getReturnType()))) {
						throw new ExpressionException(
								"Invalid return type for group-by stats expression: "
										+ statsExpr.getReturnType());
					}

					for (int i = 0; i < groupByStatsExpressions.size(); i++) {
						if (statsExpr.equals(groupByStatsExpressions.get(i))) {
							aggrExpr.setStatsIndex(i);
							break;
						}
					}
					if (aggrExpr.getStatsIndex() == GroupByAggregationExpression.INVALID_STATS_INDEX) {
						aggrExpr.setStatsIndex(groupByStatsExpressions.size());
						groupByStatsExpressions.add(statsExpr);
					}
					return ExpressionVisitor.ReturnCode.SKIP_SUBTREE;
				}
				return ExpressionVisitor.ReturnCode.CONTINUE;
			}
		};
		for (Expression expr : selectExpressions) {
			ExpressionTraversal.traverse(expr, visitor);
		}
		if (havingExpression != null) {
			ExpressionTraversal.traverse(havingExpression, visitor);
		}

		return groupByStatsExpressions;
	}

	private Comparator<GroupByCombinedKey> processSortSpecs(
			List<Expression> selectExpressions, int[] orderByIndices,
			final boolean[] ascending) {
		if (orderByIndices == null) {
			return GroupByCombinedKey.DEFAULT_COMPARATOR;
		}
		int count = orderByIndices.length;
		final int[] keySorting = new int[count];
		for (int i = 0; i < count; i++) {
			Expression expr = (Expression) selectExpressions
					.get(orderByIndices[i]);
			if (!(expr instanceof GroupByKeyExpression)) {
				return null;
			}
			keySorting[i] = ((GroupByKeyExpression) expr).getKeyIndex();
		}

		return new Comparator<GroupByCombinedKey>() {
			public int compare(GroupByCombinedKey left, GroupByCombinedKey right) {
				int comp = left.getKeys().length - right.getKeys().length;
				if (comp != 0) {
					return comp;
				}
				for (int i = 0; i < keySorting.length; i++) {
					int keyIndex = keySorting[i];
					comp = EvaluationResult.NULL_AS_MAX_COMPARATOR
							.compare(left.getKeys()[keyIndex],
									right.getKeys()[keyIndex]);

					if (comp != 0) {
						return ascending[i] != false ? comp : -comp;
					}
				}
				return 0;
			}
		};
	}

	private Comparator<EvaluationResult[]> createComparator(
			final int[] orderByIndices, final boolean[] ascending) {
		return new Comparator<EvaluationResult[]>() {
			public int compare(EvaluationResult[] left, EvaluationResult[] right) {
				int comp = left.length - right.length;
				if (comp == 0) {
					for (int i = 0; i < orderByIndices.length; i++) {
						int index = orderByIndices[i];
						comp = EvaluationResult.NULL_AS_MAX_COMPARATOR.compare(
								left[index], right[index]);
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
		};
	}

	private boolean validateAndDecorateParameters(Scan[] scans,
			List<Expression> groupByKeyExpressions,
			List<Expression> groupByStatsExpressions) throws IOException,
			ExpressionException {
		for (Scan scan : scans) {
			if ((scan != null)
					&& ((!Bytes.equals(scan.getStartRow(), scan.getStopRow())) || (Bytes
							.equals(scan.getStartRow(),
									HConstants.EMPTY_START_ROW)))
					&& ((Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) <= 0) || (Bytes
							.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)))) {
				continue;
			}

			throw new IOException(
					"GroupBy client Exception: Startrow should be smaller than Stoprow");
		}

		List<Pair<byte[], byte[]>> columns = new ArrayList<Pair<byte[], byte[]>>();
		boolean isConstantGroupByKey = true;
		for (Expression keyExpr : groupByKeyExpressions) {
			ColumnVisitor visitor = new ColumnVisitor();
			ExpressionTraversal.traverse(keyExpr, visitor);
			for (Pair<byte[], byte[]> columnPair : visitor.getColumnSet()) {
				if ((columnPair.getFirst() == null)
						|| (columnPair.getSecond() == null))
					throw new IllegalArgumentException(
							"ColumnValueExpression cannot specify null family or qualifier");
				columns.add(columnPair);
			}

			if ((isConstantGroupByKey) && (visitor.getColumnSet().isEmpty())) {
				IsConstantVisitor constVisitor = new IsConstantVisitor();
				ExpressionTraversal.traverse(keyExpr, constVisitor);
				isConstantGroupByKey = constVisitor.isConstant();
			} else {
				isConstantGroupByKey = false;
			}
		}

		ExpressionVisitor groupByExprCheckVisitor = new ExpressionVisitor() {
			public ExpressionVisitor.ReturnCode processExpression(
					Expression expression) throws ExpressionException {
				if ((expression instanceof GroupByKeyExpression)) {
					throw new ExpressionException(
							"The stats expression cannot contain GroupByKeyExpression");
				}
				if ((expression instanceof GroupByAggregationExpression)) {
					throw new ExpressionException(
							"The stats expression cannot contain GroupByAggregationExpression");
				}
				return ExpressionVisitor.ReturnCode.CONTINUE;
			}
		};
		if (groupByStatsExpressions != null) {
			for (Expression statsExpr : groupByStatsExpressions) {
				ExpressionTraversal
						.traverse(statsExpr, groupByExprCheckVisitor);
				ColumnVisitor visitor = new ColumnVisitor();
				ExpressionTraversal.traverse(statsExpr, visitor);
				for (Pair<byte[], byte[]> columnPair : visitor.getColumnSet()) {
					if ((columnPair.getFirst() == null)
							|| (columnPair.getSecond() == null))
						throw new IllegalArgumentException(
								"ColumnValueExpression cannot specify null family or qualifier");
					columns.add(columnPair);
				}
			}
		}

		for (Scan scan : scans) {
			scan.setCacheBlocks(false);
			scan.getFamilyMap().clear();
			if ((columns.isEmpty()) && (!scan.hasFilter())) {
				log.debug("Use FirstKeyOnlyFilter");
				scan.setFilter(new FirstKeyOnlyFilter());
			}
		}

		return isConstantGroupByKey;
	}

	public int getMapSize() {
		return this.MAP_SIZE;
	}

	public float getMapLoadFactor() {
		return this.LOAD_FACTOR;
	}

	public void setMapLoadFactor(float loadFactor) {
		this.LOAD_FACTOR = loadFactor;
	}

	public void setMapSize(int mapSize) {
		this.MAP_SIZE = mapSize;
	}
}