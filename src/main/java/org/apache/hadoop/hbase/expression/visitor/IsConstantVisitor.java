package org.apache.hadoop.hbase.expression.visitor;

import org.apache.hadoop.hbase.expression.ColumnValueExpression;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.ExpressionException;
import org.apache.hadoop.hbase.expression.RowExpression;

public class IsConstantVisitor implements ExpressionVisitor {
	private boolean isConstant = true;

	public boolean isConstant() {
		return this.isConstant;
	}

	public ExpressionVisitor.ReturnCode processExpression(Expression expression)
			throws ExpressionException {
		if ((expression instanceof ColumnValueExpression))
			this.isConstant = false;
		else if ((expression instanceof RowExpression)) {
			this.isConstant = false;
		}
		return this.isConstant ? ExpressionVisitor.ReturnCode.CONTINUE
				: ExpressionVisitor.ReturnCode.TERMINATE;
	}
}