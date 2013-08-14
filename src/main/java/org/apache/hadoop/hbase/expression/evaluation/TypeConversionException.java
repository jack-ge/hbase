package org.apache.hadoop.hbase.expression.evaluation;

public class TypeConversionException extends EvaluationException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TypeConversionException(Object obj, Class<?> expectedType) {
		super("Could not get object: " + obj + " as type: "
				+ expectedType.getName());
	}

	public TypeConversionException(String objectType, Class<?> expectedType) {
		super("Could not convert type: " + objectType + " to type: "
				+ expectedType.getName());
	}
}