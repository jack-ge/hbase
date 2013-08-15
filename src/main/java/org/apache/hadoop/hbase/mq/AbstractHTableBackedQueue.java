package org.apache.hadoop.hbase.mq;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTableInterface;

public abstract class AbstractHTableBackedQueue implements IQueue {
	public abstract HTableInterface getTable() throws IOException;
}