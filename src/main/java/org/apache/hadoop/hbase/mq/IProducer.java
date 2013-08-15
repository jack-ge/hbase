package org.apache.hadoop.hbase.mq;

import java.io.Closeable;
import java.io.IOException;

public abstract interface IProducer extends Closeable {
	public abstract void produce(Message paramMessage) throws IOException;
}