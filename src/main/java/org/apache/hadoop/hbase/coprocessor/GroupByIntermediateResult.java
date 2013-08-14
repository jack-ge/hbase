package org.apache.hadoop.hbase.coprocessor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.expression.evaluation.StatsValue;

public class GroupByIntermediateResult implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private List<EvaluationResult[]> topList;
	private Map<GroupByCombinedKey, List<StatsValue>> statsMap;

	public GroupByIntermediateResult() {
	}

	public GroupByIntermediateResult(List<EvaluationResult[]> topList,
			Map<GroupByCombinedKey, List<StatsValue>> statsMap) {
		this.topList = topList;
		this.statsMap = statsMap;
	}

	public List<EvaluationResult[]> getTopList() {
		return this.topList;
	}

	public void setTopList(List<EvaluationResult[]> topList) {
		this.topList = topList;
	}

	public Map<GroupByCombinedKey, List<StatsValue>> getStatsMap() {
		return this.statsMap;
	}

	public void setStatsMap(Map<GroupByCombinedKey, List<StatsValue>> statsMap) {
		this.statsMap = statsMap;
	}
}