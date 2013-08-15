package org.apache.hadoop.hbase.mq;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.OrdinalIncrementalSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;

public class MessageQueueAdmin {
	private HBaseAdmin admin;
	private final int DEFAULT_PREFIX_LEN = 2;

	public MessageQueueAdmin(HBaseAdmin admin) {
		this.admin = admin;
	}

	public void createQueue(HTableDescriptor desc, int partitionCount)
			throws IOException {
		if ((0 == partitionCount) || (null == desc)) {
			throw new IllegalArgumentException(
					"partition count or HTableDescriptor cannot be null");
		}

		HColumnDescriptor[] familes = desc.getColumnFamilies();
		if ((null == familes) || (familes.length > 1)) {
			throw new IOException("Message queue can only have one family");
		}

		byte[][] splitKey = new byte[partitionCount - 1][];
		for (int i = 1; i < partitionCount; i++) {
			splitKey[(i - 1)] = Bytes.toBytes((short) i);
		}

		desc.setValue("TABLETYPE", "MESSAGE_QUEUE");

//		desc.setRollingScan(true);

		desc.setValue("SPLIT_POLICY",
				OrdinalIncrementalSplitPolicy.class.getName());

		desc.setValue("ordinal_incremental_split_policy.ordinal_length",
				String.valueOf(DEFAULT_PREFIX_LEN));

		desc.setValue("hbase.hregion.max.filesize",
				String.valueOf(9223372036854775807L));

		desc.setValue("hbase.hstore.blockingStoreFiles",
				String.valueOf(2147483647));

		desc.setValue("hbase.hstore.compaction.min", String.valueOf(2147483647));

		desc.setValue("COPROCESSOR$999999",
				"|" + MessageQueueCoprocessor.class.getCanonicalName() + "|"
						+ 1073741822);

		Arrays.sort(splitKey, Bytes.BYTES_RAWCOMPARATOR);
		this.admin.createTable(desc, splitKey);
	}

	public void deleteQueue(String queueName) throws IOException {
		if (this.admin.tableExists(queueName)) {
			this.admin.disableTable(queueName);
			this.admin.deleteTable(Bytes.toBytes(queueName));
		}
	}

	public void increasePartition(String queueName) throws IOException,
			InterruptedException {
		byte[] tableName = Bytes.toBytes(queueName);
		List<HRegionInfo> infos = this.admin.getTableRegions(tableName);
		if ((null == infos) || (infos.size() == 0)) {
			return;
		}

		int partitionNumber = infos.size();
		int newPartitionId = partitionNumber;
		byte[] split = Bytes.toBytes((short) newPartitionId);
		this.admin.split(tableName, split);
	}
}