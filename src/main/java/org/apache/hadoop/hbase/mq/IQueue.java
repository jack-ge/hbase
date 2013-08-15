package org.apache.hadoop.hbase.mq;

import java.io.Closeable;
import java.io.IOException;

public abstract interface IQueue extends Closeable {
	public abstract IConsumer createConsumer() throws IOException;

	public abstract IProducer createProducer(IPartitioner paramIPartitioner)
			throws IOException;

	public abstract int getPartitionNumber() throws IOException;

	public abstract String getName();
}