//package org.apache.hadoop.hbase.expression;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericDatumReader;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.io.BinaryDecoder;
//import org.apache.avro.io.DecoderFactory;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hbase.expression.evaluation.BytesReference;
//import org.apache.hadoop.hbase.expression.evaluation.EvaluationContext;
//import org.apache.hadoop.hbase.expression.evaluation.EvaluationException;
//import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
//import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult.ResultType;
//import org.apache.hadoop.hbase.util.Bytes;
//
//public class AvroColumnValueExpression implements Expression {
//	public static final Log LOG = LogFactory
//			.getLog(AvroColumnValueExpression.class.getName());
//	private byte[] family;
//	private byte[] qualifier;
//	private byte[] colName;
//	private byte[] schemaString;
//	GenericDatumReader reader;
//	Schema schema;
//	BinaryDecoder decoder;
//	GenericRecord result;
//	EvaluationResult.ResultType returnType = EvaluationResult.ResultType.BYTE;
//
//	public AvroColumnValueExpression() {
//	}
//
//	public AvroColumnValueExpression(byte[] family, byte[] qualifier,
//			byte[] colName, byte[] schemaString) {
//		this.family = family;
//		this.qualifier = qualifier;
//		this.colName = colName;
//		this.schemaString = schemaString;
//		this.schema = Schema.parse(new String(this.schemaString));
//		this.reader = new GenericDatumReader(this.schema);
//	}
//
//	public byte[] getFamily() {
//		return this.family;
//	}
//
//	public byte[] getQualifier() {
//		return this.qualifier;
//	}
//
//	public EvaluationResult evaluate(EvaluationContext context)
//			throws EvaluationException {
//		if ((this.family == null) || (this.qualifier == null))
//			throw new EvaluationException("Missing required arguments");
//		if (this.reader == null) {
//			this.schema = Schema.parse(new String(this.schemaString));
//			this.reader = new GenericDatumReader(this.schema);
//			LOG.info("avro_schema=" + new String(this.schemaString));
//		}
//		BytesReference ref = context
//				.getColumnValue(this.family, this.qualifier);
//		if (ref == null) {
//			EvaluationResult result = new EvaluationResult(null, null);
//			this.returnType = result.getResultType();
//			return result;
//		}
//		this.decoder = DecoderFactory.get().binaryDecoder(ref.getReference(),
//				ref.getOffset(), ref.getLength(), this.decoder);
//		try {
//			this.result = ((GenericRecord) this.reader.read(null, this.decoder));
//		} catch (IOException e) {
//			throw new EvaluationException(e.getMessage());
//		}
//		Object obj = this.result.get(new String(this.colName));
//		EvaluationResult result = new EvaluationResult(obj, null);
//		this.returnType = result.getResultType();
//		return result;
//	}
//
//	public boolean equals(Object expr) {
//		if (this == expr)
//			return true;
//		if ((expr == null) || (!(expr instanceof AvroColumnValueExpression)))
//			return false;
//		AvroColumnValueExpression other = (AvroColumnValueExpression) expr;
//		return (Bytes.equals(this.family, other.family))
//				&& (Bytes.equals(this.qualifier, other.qualifier))
//				&& (Bytes.equals(this.colName, other.colName));
//	}
//
//	public int hashCode() {
//		int result = this.family == null ? 1 : Bytes.hashCode(this.family);
//		result = result * 31
//				+ (this.qualifier == null ? 1 : Bytes.hashCode(this.qualifier));
//		return result;
//	}
//
//	public String toString() {
//		return "avroColumnValue(\"" + Bytes.toString(this.family) + "\", \""
//				+ Bytes.toString(this.qualifier) + "\", \""
//				+ Bytes.toString(this.colName) + "\")";
//	}
//
//	public void readFields(DataInput in) throws IOException {
//		this.family = Bytes.readByteArray(in);
//		this.qualifier = Bytes.readByteArray(in);
//		this.colName = Bytes.readByteArray(in);
//		this.schemaString = Bytes.readByteArray(in);
//	}
//
//	public void write(DataOutput out) throws IOException {
//		Bytes.writeByteArray(out, this.family);
//		Bytes.writeByteArray(out, this.qualifier);
//		Bytes.writeByteArray(out, this.colName);
//		Bytes.writeByteArray(out, this.schemaString);
//	}
//
//	public EvaluationResult.ResultType getReturnType() {
//		return this.returnType;
//	}
//}