package org.apache.hadoop.hbase.expression.visitor;

import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.ExpressionException;

public abstract interface ExpressionVisitor {
	public abstract ReturnCode processExpression(Expression paramExpression)
			throws ExpressionException;

	public static enum ReturnCode {
		CONTINUE, SKIP_SUBTREE, TERMINATE;
	}
}