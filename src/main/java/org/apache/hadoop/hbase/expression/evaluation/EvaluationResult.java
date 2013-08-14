package org.apache.hadoop.hbase.expression.evaluation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class EvaluationResult implements Writable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static final Comparator<EvaluationResult> NATURAL_COMPARATOR = new Comparator<EvaluationResult>() {
		public int compare(EvaluationResult left, EvaluationResult right) {
			try {
				return EvaluationResult.compare(left, right);
			} catch (EvaluationException e) {
				throw new RuntimeException("Error occurred in comparison", e);

			}
		}
	};

	public static final Comparator<EvaluationResult> NULL_AS_MIN_COMPARATOR = new Comparator<EvaluationResult>() {
		public int compare(EvaluationResult left, EvaluationResult right) {
			try {
				return right.isNullResult() ? 1 : left.isNullResult() ? -1
						: right.isNullResult() ? 0 : EvaluationResult.compare(
								left, right);
			} catch (EvaluationException e) {
				throw new RuntimeException("Error occurred in comparison", e);

			}
		}
	};

	public static final Comparator<EvaluationResult> NULL_AS_MAX_COMPARATOR = new Comparator<EvaluationResult>() {
		public int compare(EvaluationResult left, EvaluationResult right) {
			try {
				return right.isNullResult() ? -1 : left.isNullResult() ? 1
						: right.isNullResult() ? 0 : EvaluationResult.compare(
								left, right);
			} catch (EvaluationException e) {
				throw new RuntimeException("Error occurred in comparison", e);
			}
		}
	};
	private Object result;
	private ResultType type;

	public static ResultType getObjectResultType(Object obj) {
		if (obj == null) {
			return ResultType.UNKNOWN;
		}
		if ((obj instanceof Byte))
			return ResultType.BYTE;
		if ((obj instanceof Short))
			return ResultType.SHORT;
		if ((obj instanceof Integer))
			return ResultType.INTEGER;
		if ((obj instanceof Long))
			return ResultType.LONG;
		if ((obj instanceof Float))
			return ResultType.FLOAT;
		if ((obj instanceof Double))
			return ResultType.DOUBLE;
		if ((obj instanceof BigInteger))
			return ResultType.BIGINTEGER;
		if ((obj instanceof BigDecimal))
			return ResultType.BIGDECIMAL;
		if ((obj instanceof Boolean))
			return ResultType.BOOLEAN;
		if ((obj instanceof byte[]))
			return ResultType.BYTEARRAY;
		if ((obj instanceof BytesReference))
			return ResultType.BYTESREFERENCE;
		if ((obj instanceof String)) {
			return ResultType.STRING;
		}
		return ResultType.UNKNOWN;
	}

	public static boolean isNumber(ResultType t) {
		return t.getCode() <= ResultType.BIGDECIMAL.getCode();
	}

	public static ResultType getMaxResultType(ResultType t1, ResultType t2) {
		byte c1 = t1.getCode();
		byte c2 = t2.getCode();
		if (c1 == c2) {
			return t1;
		}
		if (c1 == ResultType.BIGINTEGER.getCode())
			c1 = ResultType.BIGDECIMAL.getCode();
		if (c2 == ResultType.BIGINTEGER.getCode())
			c2 = ResultType.BIGDECIMAL.getCode();
		if (c1 == ResultType.FLOAT.getCode())
			c1 = ResultType.DOUBLE.getCode();
		if (c2 == ResultType.FLOAT.getCode()) {
			c2 = ResultType.DOUBLE.getCode();
		}
		if ((isNumber(t1)) && (isNumber(t2))) {
			return c1 < c2 ? t2 : t1;
		}
		if (((c1 == ResultType.BYTEARRAY.getCode()) && (c2 == ResultType.BYTESREFERENCE
				.getCode()))
				|| ((c2 == ResultType.BYTEARRAY.getCode()) && (c1 == ResultType.BYTESREFERENCE
						.getCode()))) {
			return ResultType.BYTEARRAY;
		}
		return ResultType.UNKNOWN;
	}

	public EvaluationResult() {
		this(null, ResultType.UNKNOWN);
	}

	public EvaluationResult(Object result, ResultType type) {
		this.result = result;
		this.type = (type == null ? getObjectResultType(result) : type);
	}

	public boolean isNullResult() {
		return this.result == null;
	}

	public ResultType getResultType() {
		return this.type;
	}

	public boolean isNumber() {
		return isNumber(this.type);
	}

	public Number asNumber() throws TypeConversionException {
		if (this.result == null) {
			return null;
		}
		if (isNumber()) {
			return (Number) this.result;
		}
		throw new TypeConversionException(this.result, Number.class);
	}

	public String asString() throws TypeConversionException {
		if (this.result == null) {
			return null;
		}
		if (this.type == ResultType.STRING) {
			return (String) this.result;
		}
		throw new TypeConversionException(this.result, String.class);
	}

	public Boolean asBoolean() throws TypeConversionException {
		if (this.result == null) {
			return null;
		}
		if (this.type == ResultType.BOOLEAN) {
			return (Boolean) this.result;
		}
		throw new TypeConversionException(this.result, Boolean.class);
	}

	public byte[] asBytes() throws TypeConversionException {
		if (this.result == null) {
			return null;
		}
		if (this.type == ResultType.BYTEARRAY) {
			return (byte[]) (byte[]) this.result;
		}
		if (this.type == ResultType.BYTESREFERENCE) {
			BytesReference ref = (BytesReference) this.result;
			return ref.toBytes();
		}

		throw new TypeConversionException(this.result, byte[].class);
	}

	public BytesReference asBytesReference() throws TypeConversionException {
		if (this.result == null) {
			return null;
		}
		if (this.type == ResultType.BYTESREFERENCE) {
			return (BytesReference) this.result;
		}
		if (this.type == ResultType.BYTEARRAY) {
			byte[] b = (byte[]) (byte[]) this.result;
			return new BytesReference(b, 0, b.length);
		}

		throw new TypeConversionException(this.result, BytesReference.class);
	}

	public BigDecimal asBigDecimal() throws TypeConversionException {
		if (this.result == null) {
			return null;
		}
		if (this.type == ResultType.BYTE) {
			return new BigDecimal(((Byte) this.result).byteValue());
		}
		if (this.type == ResultType.SHORT) {
			return new BigDecimal(((Short) this.result).shortValue());
		}
		if (this.type == ResultType.INTEGER) {
			return new BigDecimal(((Integer) this.result).intValue());
		}
		if (this.type == ResultType.LONG) {
			return new BigDecimal(((Long) this.result).longValue());
		}
		if (this.type == ResultType.FLOAT) {
			return new BigDecimal(((Float) this.result).floatValue());
		}
		if (this.type == ResultType.DOUBLE) {
			return new BigDecimal(((Double) this.result).doubleValue());
		}
		if (this.type == ResultType.BIGINTEGER) {
			return new BigDecimal((BigInteger) this.result);
		}
		if (this.type == ResultType.BIGDECIMAL) {
			return (BigDecimal) this.result;
		}
		throw new TypeConversionException(this.result, BigDecimal.class);
	}

	public Byte asByte() throws TypeConversionException {
		if (this.result == null) {
			return null;
		}
		if (isNumber()) {
			return Byte.valueOf(((Number) this.result).byteValue());
		}
		throw new TypeConversionException(this.result, Byte.class);
	}

	public Double asDouble() throws TypeConversionException {
		if (this.result == null) {
			return null;
		}
		if (isNumber()) {
			return Double.valueOf(((Number) this.result).doubleValue());
		}
		throw new TypeConversionException(this.result, Double.class);
	}

	public Float asFloat() throws TypeConversionException {
		if (this.result == null) {
			return null;
		}
		if (isNumber()) {
			return Float.valueOf(((Number) this.result).floatValue());
		}
		throw new TypeConversionException(this.result, Float.class);
	}

	public Integer asInteger() throws TypeConversionException {
		if (this.result == null) {
			return null;
		}
		if (isNumber()) {
			return Integer.valueOf(((Number) this.result).intValue());
		}
		throw new TypeConversionException(this.result, Integer.class);
	}

	public Long asLong() throws TypeConversionException {
		if (this.result == null) {
			return null;
		}
		if (isNumber()) {
			return Long.valueOf(((Number) this.result).longValue());
		}
		throw new TypeConversionException(this.result, Long.class);
	}

	public Short asShort() throws TypeConversionException {
		if (this.result == null) {
			return null;
		}
		if (isNumber()) {
			return Short.valueOf(((Number) this.result).shortValue());
		}
		throw new TypeConversionException(this.result, Short.class);
	}

	public EvaluationResult asSerializableResult() {
		if ((!isNullResult()) && (this.type == ResultType.BYTESREFERENCE)) {
			return new EvaluationResult(
					((BytesReference) this.result).toBytes(),
					ResultType.BYTEARRAY);
		}
		return this;
	}

	public int hashCode() {
		if (isNullResult()) {
			return 1;
		}
		if (this.type == ResultType.BYTEARRAY) {
			return Bytes.hashCode((byte[]) (byte[]) this.result);
		}
		return this.result.hashCode();
	}

	public String toString() {
		if (isNullResult()) {
			return null;
		}
		String res = null;
		switch (this.type.ordinal()) {
		case 1:
			res = (String) this.result;
			break;
		case 2:
			res = ((BigDecimal) this.result).toPlainString();
			break;
		case 3:
			res = Bytes.toString((byte[]) (byte[]) this.result);
			break;
		case 4:
			BytesReference ref = (BytesReference) this.result;
			res = Bytes.toString(ref.getReference(), ref.getOffset(),
					ref.getLength());
			break;
		default:
			res = this.result.toString();
		}

		return res;
	}

	public void readFields(DataInput in) throws IOException {
		this.result = HbaseObjectWritable.readObject(in, null);
		this.type = ResultType.codeToResultType(in.readByte());
	}

	public void write(DataOutput out) throws IOException {
		if ((!isNullResult()) && (this.type == ResultType.BYTESREFERENCE)) {
			BytesReference ref = (BytesReference) this.result;
			byte[] res = ref.toBytes();
			HbaseObjectWritable.writeObject(out, res, res.getClass(), null);
			out.writeByte(ResultType.BYTEARRAY.getCode());
		} else {
			HbaseObjectWritable.writeObject(
					out,
					this.result,
					this.result == null ? Serializable.class : this.result
							.getClass(), null);

			out.writeByte(this.type.getCode());
		}
	}

	public static int compare(EvaluationResult l, EvaluationResult r)
			throws EvaluationException {
		int comp = 0;
		if ((l.type == ResultType.BYTEARRAY)
				&& (r.type == ResultType.BYTEARRAY))
			comp = Bytes.compareTo((byte[]) (byte[]) l.result,
					(byte[]) (byte[]) r.result);
		else if ((l.isNumber()) && (r.isNumber()))
			comp = numberCompare(l, r);
		else if ((l.type == ResultType.STRING) && (r.type == ResultType.STRING))
			comp = ((String) l.result).compareTo((String) r.result);
		else if ((l.type == ResultType.BYTESREFERENCE)
				&& (r.type == ResultType.BYTESREFERENCE))
			comp = ((BytesReference) l.result)
					.compareTo((BytesReference) r.result);
		else if ((l.type == ResultType.BYTEARRAY)
				&& (r.type == ResultType.BYTESREFERENCE)) {
			comp = new BytesReference((byte[]) (byte[]) l.result, 0,
					((byte[]) (byte[]) l.result).length)
					.compareTo((BytesReference) r.result);
		} else if ((l.type == ResultType.BYTESREFERENCE)
				&& (r.type == ResultType.BYTEARRAY)) {
			comp = ((BytesReference) l.result).compareTo(new BytesReference(
					(byte[]) (byte[]) r.result, 0,
					((byte[]) (byte[]) r.result).length));
		} else if ((l.type == ResultType.BOOLEAN)
				&& (r.type == ResultType.BOOLEAN))
			comp = ((Boolean) l.result).compareTo((Boolean) r.result);
		else {
			throw new EvaluationException("Unsupported comparison between "
					+ l.getClass() + " and " + r.getClass());
		}

		return comp;
	}

	public static EvaluationResult numberAdd(EvaluationResult l,
			EvaluationResult r) throws EvaluationException {
		if ((l.result == null) || (r.result == null)) {
			return new EvaluationResult();
		}
		ResultType t = getMaxResultType(l.type, r.type);
		EvaluationResult res = null;

		switch (t.ordinal()) {
		case 2:
			res = new EvaluationResult(l.asBigDecimal().add(r.asBigDecimal()),
					t);
			break;
		case 5:
			res = new EvaluationResult(l.asBigDecimal().add(r.asBigDecimal()),
					t);
			break;
		case 6:
			res = new EvaluationResult(Double.valueOf(l.asDouble()
					.doubleValue() + r.asDouble().doubleValue()), t);
			break;
		case 7:
			res = new EvaluationResult(Float.valueOf(l.asFloat().floatValue()
					+ r.asFloat().floatValue()), t);
			break;
		case 8:
			res = new EvaluationResult(Long.valueOf(l.asLong().longValue()
					+ r.asLong().longValue()), t);
			break;
		case 9:
			res = new EvaluationResult(Integer.valueOf(l.asInteger().intValue()
					+ r.asInteger().intValue()), t);
			break;
		case 10:
			res = new EvaluationResult(Integer.valueOf(l.asShort().shortValue()
					+ r.asShort().shortValue()), t);
			break;
		case 11:
			res = new EvaluationResult(Integer.valueOf(l.asByte().byteValue()
					+ r.asByte().byteValue()), t);
			break;
		case 3:
		case 4:
		default:
			throw new EvaluationException("Unsupported addition between "
					+ l.type + " and " + r.type);
		}

		return res;
	}

	public static EvaluationResult numberSubtract(EvaluationResult l,
			EvaluationResult r) throws EvaluationException {
		if ((l.result == null) || (r.result == null)) {
			return new EvaluationResult();
		}
		ResultType t = getMaxResultType(l.type, r.type);
		EvaluationResult res = null;

		switch (t.ordinal()) {
		case 2:
			res = new EvaluationResult(l.asBigDecimal().subtract(
					r.asBigDecimal()), t);
			break;
		case 5:
			res = new EvaluationResult(l.asBigDecimal().subtract(
					r.asBigDecimal()), t);
			break;
		case 6:
			res = new EvaluationResult(Double.valueOf(l.asDouble()
					.doubleValue() - r.asDouble().doubleValue()), t);
			break;
		case 7:
			res = new EvaluationResult(Float.valueOf(l.asFloat().floatValue()
					- r.asFloat().floatValue()), t);
			break;
		case 8:
			res = new EvaluationResult(Long.valueOf(l.asLong().longValue()
					- r.asLong().longValue()), t);
			break;
		case 9:
			res = new EvaluationResult(Integer.valueOf(l.asInteger().intValue()
					- r.asInteger().intValue()), t);
			break;
		case 10:
			res = new EvaluationResult(Integer.valueOf(l.asShort().shortValue()
					- r.asShort().shortValue()), t);
			break;
		case 11:
			res = new EvaluationResult(Integer.valueOf(l.asByte().byteValue()
					- r.asByte().byteValue()), t);
			break;
		case 3:
		case 4:
		default:
			throw new EvaluationException("Unsupported subtraction between "
					+ l.type + " and " + r.type);
		}

		return res;
	}

	public static EvaluationResult numberMultiply(EvaluationResult l,
			EvaluationResult r) throws EvaluationException {
		if ((l.result == null) || (r.result == null)) {
			return new EvaluationResult();
		}
		ResultType t = getMaxResultType(l.type, r.type);
		EvaluationResult res = null;

		switch (t.ordinal()) {
		case 2:
			res = new EvaluationResult(l.asBigDecimal().multiply(
					r.asBigDecimal()), t);
			break;
		case 5:
			res = new EvaluationResult(l.asBigDecimal().multiply(
					r.asBigDecimal()), t);
			break;
		case 6:
			res = new EvaluationResult(Double.valueOf(l.asDouble()
					.doubleValue() * r.asDouble().doubleValue()), t);
			break;
		case 7:
			res = new EvaluationResult(Float.valueOf(l.asFloat().floatValue()
					* r.asFloat().floatValue()), t);
			break;
		case 8:
			res = new EvaluationResult(Long.valueOf(l.asLong().longValue()
					* r.asLong().longValue()), t);
			break;
		case 9:
			res = new EvaluationResult(Integer.valueOf(l.asInteger().intValue()
					* r.asInteger().intValue()), t);
			break;
		case 10:
			res = new EvaluationResult(Integer.valueOf(l.asShort().shortValue()
					* r.asShort().shortValue()), t);
			break;
		case 11:
			res = new EvaluationResult(Integer.valueOf(l.asByte().byteValue()
					* r.asByte().byteValue()), t);
			break;
		case 3:
		case 4:
		default:
			throw new EvaluationException("Unsupported multiplication between "
					+ l.type + " and " + r.type);
		}

		return res;
	}

	public static EvaluationResult numberDivide(EvaluationResult l,
			EvaluationResult r) throws EvaluationException {
		if ((l.result == null) || (r.result == null)) {
			return new EvaluationResult();
		}
		ResultType t = getMaxResultType(l.type, r.type);
		EvaluationResult res = null;

		switch (t.ordinal()) {
		case 2:
			res = new EvaluationResult(l.asBigDecimal()
					.divide(r.asBigDecimal()), t);
			break;
		case 5:
			res = new EvaluationResult(l.asBigDecimal()
					.divide(r.asBigDecimal()), t);
			break;
		case 6:
			res = new EvaluationResult(Double.valueOf(l.asDouble()
					.doubleValue() / r.asDouble().doubleValue()), t);
			break;
		case 7:
			res = new EvaluationResult(Float.valueOf(l.asFloat().floatValue()
					/ r.asFloat().floatValue()), t);
			break;
		case 8:
			res = new EvaluationResult(Long.valueOf(l.asLong().longValue()
					/ r.asLong().longValue()), t);
			break;
		case 9:
			res = new EvaluationResult(Integer.valueOf(l.asInteger().intValue()
					/ r.asInteger().intValue()), t);
			break;
		case 10:
			res = new EvaluationResult(Integer.valueOf(l.asShort().shortValue()
					/ r.asShort().shortValue()), t);
			break;
		case 11:
			res = new EvaluationResult(Integer.valueOf(l.asByte().byteValue()
					/ r.asByte().byteValue()), t);
			break;
		case 3:
		case 4:
		default:
			throw new EvaluationException("Unsupported division between "
					+ l.type + " and " + r.type);
		}

		return res;
	}

	public static EvaluationResult numberRemainder(EvaluationResult l,
			EvaluationResult r) throws EvaluationException {
		if ((l.result == null) || (r.result == null)) {
			return new EvaluationResult();
		}
		ResultType t = getMaxResultType(l.type, r.type);
		EvaluationResult res = null;

		switch (t.ordinal()) {
		case 2:
			res = new EvaluationResult(l.asBigDecimal().remainder(
					r.asBigDecimal()), t);
			break;
		case 5:
			res = new EvaluationResult(l.asBigDecimal().remainder(
					r.asBigDecimal()), t);
			break;
		case 6:
			res = new EvaluationResult(Double.valueOf(l.asDouble()
					.doubleValue() % r.asDouble().doubleValue()), t);
			break;
		case 7:
			res = new EvaluationResult(Float.valueOf(l.asFloat().floatValue()
					% r.asFloat().floatValue()), t);
			break;
		case 8:
			res = new EvaluationResult(Long.valueOf(l.asLong().longValue()
					% r.asLong().longValue()), t);
			break;
		case 9:
			res = new EvaluationResult(Integer.valueOf(l.asInteger().intValue()
					% r.asInteger().intValue()), t);
			break;
		case 10:
			res = new EvaluationResult(Integer.valueOf(l.asShort().shortValue()
					% r.asShort().shortValue()), t);
			break;
		case 11:
			res = new EvaluationResult(Integer.valueOf(l.asByte().byteValue()
					% r.asByte().byteValue()), t);
			break;
		case 3:
		case 4:
		default:
			throw new EvaluationException(
					"Unsupported remainder operation between " + l.type
							+ " and " + r.type);
		}

		return res;
	}

	protected static int numberCompare(EvaluationResult l, EvaluationResult r)
			throws EvaluationException {
		ResultType t = getMaxResultType(l.type, r.type);
		int res = 0;

		switch (t.ordinal()) {
		case 2:
			res = l.asBigDecimal().compareTo(r.asBigDecimal());
			break;
		case 5:
			res = l.asBigDecimal().compareTo(r.asBigDecimal());
			break;
		case 6:
			res = l.asDouble().compareTo(r.asDouble());
			break;
		case 7:
			res = l.asFloat().compareTo(r.asFloat());
			break;
		case 8:
			res = l.asLong().compareTo(r.asLong());
			break;
		case 9:
			res = l.asInteger().compareTo(r.asInteger());
			break;
		case 10:
			res = l.asShort().compareTo(r.asShort());
			break;
		case 11:
			res = l.asByte().compareTo(r.asByte());
			break;
		case 3:
		case 4:
		default:
			throw new EvaluationException(
					"Unsupported number comparison between " + l.type + " and "
							+ r.type);
		}

		return res;
	}

	public static enum ResultType {
		BYTE((byte) 0), SHORT((byte) 4), INTEGER((byte) 8), LONG((byte) 12), FLOAT(
				(byte) 16), DOUBLE((byte) 20), BIGINTEGER((byte) 24), BIGDECIMAL(
				(byte) 28), BOOLEAN((byte) 32), BYTEARRAY((byte) 36), BYTESREFERENCE(
				(byte) 40), STRING((byte) 44),

		UNKNOWN((byte) -1);

		private final byte code;

		private ResultType(byte c) {
			this.code = c;
		}

		public byte getCode() {
			return this.code;
		}

		public static ResultType codeToResultType(byte b) {
			for (ResultType t : values()) {
				if (t.getCode() == b) {
					return t;
				}
			}
			throw new RuntimeException("Unknown code " + b);
		}
	}
}