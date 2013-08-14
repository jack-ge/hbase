package org.apache.hadoop.hbase.expression.visitor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.ExpressionException;

public class ExpressionTraversal {
	public static void traverse(Expression expression, ExpressionVisitor visitor)
			throws ExpressionException {
		traverseInternal(expression, visitor);
	}

	private static ExpressionVisitor.ReturnCode traverseInternal(
			Expression expression, ExpressionVisitor visitor)
			throws ExpressionException {
		ExpressionVisitor.ReturnCode code = visitor
				.processExpression(expression);
		if (code == ExpressionVisitor.ReturnCode.SKIP_SUBTREE)
			return ExpressionVisitor.ReturnCode.CONTINUE;
		if (code == ExpressionVisitor.ReturnCode.TERMINATE) {
			return code;
		}
		List<Expression> containing = getContainingExpressions(expression);
		for (Expression expr : containing) {
			code = traverseInternal(expr, visitor);
			if (code == ExpressionVisitor.ReturnCode.TERMINATE) {
				return code;
			}
		}
		return ExpressionVisitor.ReturnCode.CONTINUE;
	}

	private static List<Expression> getContainingExpressions(
			Expression expression) {
		List<Expression> ret = new ArrayList<Expression>();
		Field[] fields = expression.getClass().getDeclaredFields();
		for (Field field : fields) {
			field.setAccessible(true);
			if (!Expression.class.isAssignableFrom(field.getType()))
				continue;
			try {
				ret.add((Expression) field.get(expression));
			} catch (IllegalArgumentException ignored) {
			} catch (IllegalAccessException ignored) {
			}
		}
		return ret;
	}
}