package org.apache.hadoop.hbase.blobStore.compactions;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.blobStore.BlobFilePath;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SweepPartitioner extends Partitioner<Text, KeyValue> {
	public int getPartition(Text filePath, KeyValue kv, int numPartitions) {
		BlobFilePath blobPath = BlobFilePath.create(filePath.toString());
		String date = blobPath.getDate();
		int hash = date.hashCode();
		return (hash & 0x7FFFFFFF) % numPartitions;
	}
}