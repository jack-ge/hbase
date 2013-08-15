package org.apache.hadoop.hbase.mq;

public class KeyHashPartitioner implements IPartitioner {
	public int getPartition(byte[] rowKey, int numOfPartitions) {
		int hash = 1;
		if ((rowKey == null) || (rowKey.length == 0)) {
			return 0;
		}
		for (int i = 0; i < rowKey.length; i++) {
			hash = 31 * hash + rowKey[i];
		}
		hash &= 2147483647;
		return hash % numOfPartitions;
	}
}