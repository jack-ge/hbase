package org.apache.hadoop.hbase.mq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Consumer implements IConsumer {
	private HTableInterface table;
	private final int DEFAULT_CACHING = 1000;
	private int caching = 1000;
	private Offset cachedNextOffset;
	private ResultScanner cachedScanner;
	private int cachedPartitionId = -1;

	protected Consumer(AbstractHTableBackedQueue queue) throws IOException {
		this.table = queue.getTable();
	}

	public void setCaching(int caching) {
		this.caching = caching;
	}

	public synchronized Response fetch(int partition, Offset offset, int limit)
			throws IOException {
		ResultScanner activeScanner = null;

		if ((partition == this.cachedPartitionId)
				&& (0 == offset.compareTo(this.cachedNextOffset))
				&& (null != this.cachedScanner)) {
			activeScanner = this.cachedScanner;
		} else {
			if (null != this.cachedScanner) {
				this.cachedScanner.close();
				this.cachedScanner = null;
			}

			byte[] newRow = new byte[2 + offset.get().length];
			Bytes.putShort(newRow, 0, (short) partition);
			System.arraycopy(offset.get(), 0, newRow, 2, offset.get().length);

			byte[] stopRow = new byte[2];
			Bytes.putShort(stopRow, 0, (short) (partition + 1));
			Scan scan = new Scan();
			scan.setStartRow(newRow);
			scan.setStopRow(stopRow);
			scan.setCaching(this.caching);
			activeScanner = this.table.getScanner(scan);
		}
		Result[] results = activeScanner.next(limit);
		this.cachedPartitionId = partition;
		this.cachedScanner = activeScanner;
		if (null == results) {
			this.cachedNextOffset = null;
			return null;
		}

		this.cachedNextOffset = getNextOffset(results);
		return createResponse(results, this.cachedNextOffset);
	}

	private Offset getNextOffset(Result[] results) {
		if ((null == results) || (results.length == 0)) {
			return null;
		}

		byte[] lastRow = results[(results.length - 1)].raw()[0].getRow();

		Offset nextOffset = new Offset(getImmediateNext(lastRow));
		return nextOffset;
	}

	private Response createResponse(Result[] results, Offset nextOffset)
			throws IOException {
		if ((null == results) || (results.length == 0)) {
			return null;
		}

		List messages = new ArrayList();
		for (int i = 0; i < results.length; i++) {
			Result result = results[i];
			List kvs = result.list();
			if ((null == kvs) || (kvs.size() == 0)) {
				continue;
			}
			byte[] row = result.getRow();
			int newRowLength = row.length - 2 - 8 - 2;

			if (newRowLength <= 0) {
				throw new IOException(
						"The retrived response length is invalid,please make sure this table is a message queue");
			}

			byte[] newRow = new byte[newRowLength];

			System.arraycopy(row, 12, newRow, 0, newRow.length);

			Message msg = new Message(newRow);
			messages.add(msg);
		}

		return new Response(messages, nextOffset);
	}

	private byte[] getImmediateNext(byte[] row) {
		byte[] result = new byte[row.length - 2 + 1];
		System.arraycopy(row, 2, result, 0, result.length - 1);

		result[(result.length - 1)] = 0;
		return result;
	}

	public void close() throws IOException {
		if (null != this.cachedScanner) {
			this.cachedScanner.close();
			this.cachedScanner = null;
		}
		this.table.close();
	}
}