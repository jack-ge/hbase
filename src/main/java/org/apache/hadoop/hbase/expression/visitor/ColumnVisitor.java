package org.apache.hadoop.hbase.expression.visitor;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hbase.expression.ColumnValueExpression;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.ExpressionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class ColumnVisitor implements ExpressionVisitor {
	private Set<Pair<byte[], byte[]>> columnSet = new TreeSet<Pair<byte[], byte[]>>(
			new Comparator<Pair<byte[], byte[]>>() {
				public int compare(Pair<byte[], byte[]> left,
						Pair<byte[], byte[]> right) {
					int comp = Bytes.equals((byte[]) left.getFirst(),
							(byte[]) right.getFirst()) ? 0
							: Bytes.compareTo((byte[]) left.getFirst(),
									(byte[]) right.getFirst());

					if (comp == 0) {
						comp = Bytes.equals((byte[]) left.getSecond(),
								(byte[]) right.getSecond()) ? 0
								: Bytes.compareTo((byte[]) left.getSecond(),
										(byte[]) right.getSecond());
					}

					return comp;
				}
			});

	public Set<Pair<byte[], byte[]>> getColumnSet() {
		return this.columnSet;
	}

	public ExpressionVisitor.ReturnCode processExpression(Expression expression)
			throws ExpressionException {
		if ((expression instanceof ColumnValueExpression)) {
			ColumnValueExpression columnValueExpr = (ColumnValueExpression) expression;
			this.columnSet.add(new Pair<byte[], byte[]>(columnValueExpr.getFamily(),
					columnValueExpr.getQualifier()));

			return ExpressionVisitor.ReturnCode.SKIP_SUBTREE;
		}
		return ExpressionVisitor.ReturnCode.CONTINUE;
	}
}