package org.apache.hadoop.hbase.blobStore.compactions;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.mapreduce.Mapper;

public class SweepMapper extends TableMapper<Text, KeyValue> {
	protected void setup(
			Mapper<ImmutableBytesWritable, Result, Text, KeyValue>.Context context)
			throws IOException, InterruptedException {
	}

	protected void cleanup(
			Mapper<ImmutableBytesWritable, Result, Text, KeyValue>.Context context)
			throws IOException, InterruptedException {
	}

	public void map(
			ImmutableBytesWritable r,
			Result columns,
			Mapper<ImmutableBytesWritable, Result, Text, KeyValue>.Context context)
			throws IOException {
		if (columns != null) {
			KeyValue[] kvList = columns.raw();
			if ((kvList != null) && (kvList.length > 0))
				for (KeyValue kv : kvList) {
					byte[] referenceValue = kv.getValue();
					if (referenceValue.length < 1) {
						return;
					}
					byte blobStoreVersion = referenceValue[0];
					if (blobStoreVersion > 0) {
						throw new VersionMismatchException((byte) 0, blobStoreVersion);
					}

					String fileName = Bytes.toString(referenceValue, 1,
							referenceValue.length - 1);

					KeyValue keyOnly = kv.createKeyOnly(false);

					keyOnly.setType(KeyValue.Type.Put);
					try {
						context.write(new Text(fileName), keyOnly);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
		}
	}
}