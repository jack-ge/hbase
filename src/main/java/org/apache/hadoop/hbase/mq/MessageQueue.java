package org.apache.hadoop.hbase.mq;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

public class MessageQueue extends AbstractHTableBackedQueue {
	private static final Log LOG = LogFactory.getLog(HTable.class);
	private int numberOfPartitions = -1;
	private long lastSyncTime;
	private final long FIVE_MINUTES = 300000L;
	private Object sync = new Object();
	private String name;
	private HTablePool tablePool;
	private boolean closed = false;
	private IQueueInfo queueInfo;
	private String defaultTopic;

	public MessageQueue(Configuration conf, String name) throws IOException {
		this(conf, name, new HTablePool(conf, 1));
	}

	public MessageQueue(Configuration conf, String name, HTablePool pool)
			throws IOException {
		this(conf, name, pool, getQueueInfo(pool, name));
	}

	public MessageQueue(Configuration conf, String name, HTablePool pool,
			IQueueInfo queueInfo) throws IOException {
		this.tablePool = pool;

		this.name = name;
		this.queueInfo = queueInfo;
		this.defaultTopic = queueInfo.getDefaultTopic();
	}

	public Consumer createConsumer() throws IOException {
		return new Consumer(this);
	}

	public Producer createProducer(IPartitioner partitioner) throws IOException {
		return new Producer(this, this.defaultTopic, partitioner);
	}

	public int getPartitionNumber() throws IOException {
		this.numberOfPartitions = checkResync();
		return this.numberOfPartitions;
	}

	private int checkResync() throws IOException {
		long current = System.currentTimeMillis();
		if (current - this.lastSyncTime > 300000L) {
			synchronized (this.sync) {
				if (current - this.lastSyncTime > 300000L) {
					this.numberOfPartitions = this.queueInfo
							.getPartitionNumber();
					this.lastSyncTime = current;
				}
			}
		}
		return this.numberOfPartitions;
	}

	public String getName() {
		return this.name;
	}

	public void close() throws IOException {
		if (!this.closed) {
			this.closed = true;
			this.tablePool.close();
		}
	}

	public HTableInterface getTable() throws IOException {
		return this.tablePool.getTable(this.name);
	}

	private static IQueueInfo getQueueInfo(HTablePool pool, String name) {
		try {
			HTableInterface pooled = pool.getTable(name);
			Class clazz = pooled.getClass();
			Method method = clazz.getDeclaredMethod("getWrappedTable",
					new Class[0]);
			if (!method.isAccessible()) {
				method.setAccessible(true);
			}
			final HTable table = (HTable) method.invoke(pooled, new Object[0]);
			return new IQueueInfo() {
				public int getPartitionNumber() throws IOException {
					if (null == table) {
						return -1;
					}
					return table.getRegionLocations().keySet().size();
				}

				public String[] getAllTopics() throws IOException {
					HColumnDescriptor[] columns = table.getTableDescriptor()
							.getColumnFamilies();
					String[] topics = new String[columns.length];
					for (int i = 0; i < columns.length; i++) {
						topics[i] = columns[i].getNameAsString();
					}
					return topics;
				}

				public String getDefaultTopic() throws IOException {
					return getAllTopics()[0];
				}
			};
		} catch (Exception e) {
			LOG.error(
					"Cannot get getWrappedTable() method from table interface, probably this is not a PooledTable",
					e);
		}
		return null;
	}

	protected static abstract interface IQueueInfo {
		public abstract int getPartitionNumber() throws IOException;

		public abstract String getDefaultTopic() throws IOException;

		public abstract String[] getAllTopics() throws IOException;
	}
}