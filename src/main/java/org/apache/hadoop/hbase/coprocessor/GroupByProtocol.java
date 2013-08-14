package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.expression.evaluation.StatsValue;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public abstract interface GroupByProtocol extends CoprocessorProtocol {
	public static final long VERSION = 1L;

	public abstract Map<GroupByCombinedKey, List<StatsValue>> getStats(
			Scan paramScan, List<Expression> paramList1,
			List<Expression> paramList2, boolean paramBoolean)
			throws IOException;

	public abstract List<EvaluationResult[]> getTopNByRow(Scan paramScan,
			List<Expression> paramList, int[] paramArrayOfInt,
			boolean[] paramArrayOfBoolean, int paramInt) throws IOException;

	public abstract GroupByIntermediateResult getTopNByRowGrouping(
			Scan paramScan, List<Expression> paramList1,
			List<Expression> paramList2, List<Expression> paramList3,
			Expression paramExpression, int[] paramArrayOfInt,
			boolean[] paramArrayOfBoolean, int paramInt) throws IOException;

	public abstract Set<GroupByCombinedKey> distinct(Scan paramScan,
			List<Expression> paramList) throws IOException;

	public abstract void setMapSize(int paramInt);

	public abstract void setMapLoadFactor(float paramFloat);
}