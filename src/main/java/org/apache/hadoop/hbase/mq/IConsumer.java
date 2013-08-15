package org.apache.hadoop.hbase.mq;

import java.io.Closeable;
import java.io.IOException;

public abstract interface IConsumer extends Closeable {
	public abstract Response fetch(int paramInt1, Offset paramOffset,
			int paramInt2) throws IOException;
}