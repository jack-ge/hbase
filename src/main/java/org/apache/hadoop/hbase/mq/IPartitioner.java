package org.apache.hadoop.hbase.mq;

public abstract interface IPartitioner {
	public abstract int getPartition(byte[] paramArrayOfByte, int paramInt);
}