package org.apache.hadoop.hbase.expression;

import org.apache.hadoop.hbase.util.Bytes;

public class ExpressionFactory {
	public static ArithmeticExpression add(Expression left, Expression right) {
		return new ArithmeticExpression(
				ArithmeticExpression.ArithmeticOperator.ADD, left, right);
	}

	public static ArithmeticExpression subtract(Expression left,
			Expression right) {
		return new ArithmeticExpression(
				ArithmeticExpression.ArithmeticOperator.SUBTRACT, left, right);
	}

	public static ArithmeticExpression multiply(Expression left,
			Expression right) {
		return new ArithmeticExpression(
				ArithmeticExpression.ArithmeticOperator.MULTIPLY, left, right);
	}

	public static ArithmeticExpression divide(Expression left, Expression right) {
		return new ArithmeticExpression(
				ArithmeticExpression.ArithmeticOperator.DIVIDE, left, right);
	}

	public static ArithmeticExpression remainder(Expression left,
			Expression right) {
		return new ArithmeticExpression(
				ArithmeticExpression.ArithmeticOperator.REMAINDER, left, right);
	}

	// public static AvroColumnValueExpression avroColumnValue(byte[] family,
	// byte[] qualifier, byte[] colName, byte[] schemaString) {
	// return new AvroColumnValueExpression(family, qualifier, colName,
	// schemaString);
	// }

	public static CaseExpression caseWhenElse(Expression conditionExpression,
			Expression defaultExpression) {
		return new CaseExpression(conditionExpression, defaultExpression);
	}

	public static BytesPartExpression bytesPart(Expression source,
			String delimiter, int index) {
		return new BytesPartExpression(source, delimiter, index);
	}

	public static BytesPartExpression bytesPart(Expression source,
			String delimiter, Expression index) {
		return new BytesPartExpression(source, delimiter, index);
	}

	public static ColumnValueExpression columnValue(byte[] family,
			byte[] qualifier) {
		return new ColumnValueExpression(family, qualifier);
	}

	public static ColumnValueExpression columnValue(String family,
			String qualifier) {
		return new ColumnValueExpression(Bytes.toBytes(family),
				Bytes.toBytes(qualifier));
	}

	public static ComparisonExpression eq(Expression left, Expression right) {
		return new ComparisonExpression(
				ComparisonExpression.ComparisonOperator.EQUAL, left, right);
	}

	public static ComparisonExpression neq(Expression left, Expression right) {
		return new ComparisonExpression(
				ComparisonExpression.ComparisonOperator.NOT_EQUAL, left, right);
	}

	public static ComparisonExpression lt(Expression left, Expression right) {
		return new ComparisonExpression(
				ComparisonExpression.ComparisonOperator.LESS, left, right);
	}

	public static ComparisonExpression le(Expression left, Expression right) {
		return new ComparisonExpression(
				ComparisonExpression.ComparisonOperator.LESS_OR_EQUAL, left,
				right);
	}

	public static ComparisonExpression gt(Expression left, Expression right) {
		return new ComparisonExpression(
				ComparisonExpression.ComparisonOperator.GREATER, left, right);
	}

	public static ComparisonExpression ge(Expression left, Expression right) {
		return new ComparisonExpression(
				ComparisonExpression.ComparisonOperator.GREATER_OR_EQUAL, left,
				right);
	}

	public static ConstantExpression constant(Object constant) {
		return new ConstantExpression(constant);
	}

	public static GroupByAggregationExpression sum(Expression subExpression) {
		return new GroupByAggregationExpression(
				GroupByAggregationExpression.AggregationType.SUM, subExpression);
	}

	public static GroupByAggregationExpression avg(Expression subExpression) {
		return new GroupByAggregationExpression(
				GroupByAggregationExpression.AggregationType.AVG, subExpression);
	}

	public static GroupByAggregationExpression count(Expression subExpression) {
		return new GroupByAggregationExpression(
				GroupByAggregationExpression.AggregationType.COUNT,
				subExpression);
	}

	public static GroupByAggregationExpression min(Expression subExpression) {
		return new GroupByAggregationExpression(
				GroupByAggregationExpression.AggregationType.MIN, subExpression);
	}

	public static GroupByAggregationExpression max(Expression subExpression) {
		return new GroupByAggregationExpression(
				GroupByAggregationExpression.AggregationType.MAX, subExpression);
	}

	public static GroupByAggregationExpression stdDev(Expression subExpression) {
		return new GroupByAggregationExpression(
				GroupByAggregationExpression.AggregationType.STDDEV,
				subExpression);
	}

	public static GroupByKeyExpression groupByKey(Expression referenceExpression) {
		return new GroupByKeyExpression(referenceExpression);
	}

	public static InExpression in(Expression testExpression,
			ConstantExpression[] constantExpressions) {
		return new InExpression(testExpression, constantExpressions);
	}

	public static LogicalExpression and(Expression left, Expression right) {
		return new LogicalExpression(LogicalExpression.LogicalOperator.AND,
				left, right);
	}

	public static LogicalExpression or(Expression left, Expression right) {
		return new LogicalExpression(LogicalExpression.LogicalOperator.OR,
				left, right);
	}

	public static NotExpression not(Expression subExpression) {
		return new NotExpression(subExpression);
	}

	public static RowExpression row() {
		return new RowExpression();
	}

	public static StringConcatExpression stringConcat(Expression[] parts) {
		return new StringConcatExpression(parts);
	}

	public static StringMatchExpression stringMatch(Expression matchExpression,
			String regex) {
		return new StringMatchExpression(matchExpression, regex);
	}

	public static StringPartExpression stringPart(Expression source,
			String delimiter, int index) {
		return new StringPartExpression(source, delimiter, index);
	}

	public static StringPartExpression stringPart(Expression source,
			String delimiter, Expression index) {
		return new StringPartExpression(source, delimiter, index);
	}

	public static SubSequenceExpression subSequence(Expression source, int start) {
		return new SubSequenceExpression(source, start);
	}

	public static SubSequenceExpression subSequence(Expression source,
			int start, int end) {
		return new SubSequenceExpression(source, start, end);
	}

	public static SubSequenceExpression subSequence(Expression source,
			Expression start) {
		return new SubSequenceExpression(source, start);
	}

	public static SubSequenceExpression subSequence(Expression source,
			Expression start, Expression end) {
		return new SubSequenceExpression(source, start, end);
	}

	public static SubstringExpression subString(Expression source, int start) {
		return new SubstringExpression(source, start);
	}

	public static SubstringExpression subString(Expression source, int start,
			int end) {
		return new SubstringExpression(source, start, end);
	}

	public static SubstringExpression subString(Expression source,
			Expression start) {
		return new SubstringExpression(source, start);
	}

	public static SubstringExpression subString(Expression source,
			Expression start, Expression end) {
		return new SubstringExpression(source, start, end);
	}

	public static TernaryExpression ternary(Expression condExpression,
			Expression trueExpression, Expression falseExpression) {
		return new TernaryExpression(condExpression, trueExpression,
				falseExpression);
	}

	public static ToBigDecimalExpression toBigDecimal(Expression subExpression) {
		return new ToBigDecimalExpression(subExpression);
	}

	public static ToBooleanExpression toBoolean(Expression subExpression) {
		return new ToBooleanExpression(subExpression);
	}

	public static ToByteExpression toByte(Expression subExpression) {
		return new ToByteExpression(subExpression);
	}

	public static ToBytesExpression toBytes(Expression subExpression) {
		return new ToBytesExpression(subExpression);
	}

	public static ToDoubleExpression toDouble(Expression subExpression) {
		return new ToDoubleExpression(subExpression);
	}

	public static ToFloatExpression toFloat(Expression subExpression) {
		return new ToFloatExpression(subExpression);
	}

	public static ToIntegerExpression toInteger(Expression subExpression) {
		return new ToIntegerExpression(subExpression);
	}

	public static ToLongExpression toLong(Expression subExpression) {
		return new ToLongExpression(subExpression);
	}

	public static ToShortExpression toShort(Expression subExpression) {
		return new ToShortExpression(subExpression);
	}

	public static ToStringExpression toString(Expression subExpression) {
		return new ToStringExpression(subExpression);
	}
}