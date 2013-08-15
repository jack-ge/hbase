package org.apache.hadoop.hbase.mq;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Producer implements IProducer {
	private IPartitioner partitioner;
	private AbstractHTableBackedQueue queue;
	private HTableInterface table;
	private int serialNumber = 0;
	private int MAX_SERIAL = 65535;
	private boolean writeToWal = false;
	private byte[] topic;

	protected Producer(AbstractHTableBackedQueue queue, String topic,
			IPartitioner partitioner) throws IOException {
		this.partitioner = partitioner;
		this.queue = queue;
		this.topic = Bytes.toBytes(topic);
		this.table = queue.getTable();
	}

	public synchronized void produce(Message msg) throws IOException {
		int numberOfPartitions = this.queue.getPartitionNumber();

		byte[] msgBuffer = msg.getBuffer();
		if ((null == msgBuffer) || (msgBuffer.length == 0)) {
			return;
		}
		byte[] row = msgBuffer;

		this.serialNumber += 1;

		byte[] newRow = new byte[12 + row.length];

		int partition = this.partitioner.getPartition(row, numberOfPartitions);
		Bytes.putShort(newRow, 0, (short) partition);
		Bytes.putShort(newRow, 10, (short) this.serialNumber);

		System.arraycopy(row, 0, newRow, 12, row.length);

		Put put = new Put(newRow);

		KeyValue newKV = new KeyValue(newRow, this.topic, null, null);
		put.add(newKV);

		put.setWriteToWAL(this.writeToWal);
		this.table.put(put);
		if (this.serialNumber >= this.MAX_SERIAL) {
			this.table.flushCommits();
			this.serialNumber = 0;
		}
	}

	public void setAutoFlush(boolean autoFlush) {
		this.table.setAutoFlush(autoFlush, autoFlush);
	}

	public void setWriteBufferSize(long writeBufferSize) throws IOException {
		this.table.setWriteBufferSize(writeBufferSize);
	}

	protected long getWriteBufferSize() {
		return this.table.getWriteBufferSize();
	}

	public void flushCommits() throws IOException {
		this.table.flushCommits();
	}

	public void setWriteToWAL(boolean write) {
		this.writeToWal = write;
	}

	public void close() throws IOException {
		this.table.close();
	}
}