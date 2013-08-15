package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.Bytes;

public class ParallelClientScanner extends AbstractClientScanner {
	private static final int SCANNER_WAIT_TIME = 1000;
	private final HTableInterface table;
	private final int resultQueueSize;
	private final int threadCount;
	private List<Scan> scans;
	private Iterator<Scan> iter;
	private ParallelScannerWorkerThread currThread;
	private Queue<ParallelScannerWorkerThread> nextThreads;
	private boolean closed;

	public ParallelClientScanner(Configuration conf, Scan scan,
			byte[] tableName, List<byte[]> splitKeys) throws IOException {
		this(new HTable(conf, tableName), scan, splitKeys);
	}

	public ParallelClientScanner(HTableInterface table, Scan scan,
			List<byte[]> splitKeys) throws IOException {
		this.table = table;
		this.resultQueueSize = table.getConfiguration().getInt(
				"hbase.parallel.scanner.queue.size", 1000);

		this.threadCount = (table.getConfiguration().getInt(
				"hbase.parallel.scanner.thread.count", 10) - 1);

		this.scans = new ArrayList<Scan>();
		byte[] stopRow = scan.getStopRow();
		byte[] lastSplitKey = scan.getStartRow();
		Scan subScan = null;
		for (Iterator<byte[]> it = splitKeys.iterator(); it.hasNext();) {
			byte[] splitKey = (byte[]) it.next();
			if ((Bytes.compareTo(splitKey, lastSplitKey) <= 0)
					|| ((Bytes.compareTo(splitKey, stopRow) >= 0) && (!Bytes
							.equals(stopRow, HConstants.EMPTY_END_ROW)))
					|| (Bytes.equals(splitKey, HConstants.EMPTY_END_ROW))) {
				continue;
			}

			subScan = new Scan(scan);
			subScan.setStartRow(lastSplitKey);
			subScan.setStopRow(splitKey);
			subScan.setParallel(false);
			this.scans.add(subScan);
			lastSplitKey = splitKey;
		}
		subScan = new Scan(scan);
		subScan.setStartRow(lastSplitKey);
		subScan.setParallel(false);
		this.scans.add(subScan);

		this.nextThreads = new LinkedList<ParallelScannerWorkerThread> ();
		this.closed = true;
		initialize();
	}

	public ParallelClientScanner(Configuration conf, List<Scan> scans,
			byte[] tableName) throws IOException {
		this(new HTable(conf, tableName), scans);
	}

	public ParallelClientScanner(HTableInterface table, List<Scan> scanList)
			throws IOException {
		if (null == scanList) {
			throw new IOException("ScanList cannot be null");
		}
		sort(scanList);

		this.table = table;
		this.resultQueueSize = table.getConfiguration().getInt(
				"hbase.parallel.scanner.queue.size", 1000);

		this.threadCount = (table.getConfiguration().getInt(
				"hbase.parallel.scanner.thread.count", 10) - 1);

		this.scans = new ArrayList<Scan>();
		Scan subScan = null;
		for (int i = 0; i < scanList.size(); i++) {
			subScan = new Scan((Scan) scanList.get(i));
			subScan.setParallel(false);
			this.scans.add(subScan);
		}

		this.nextThreads = new LinkedList<ParallelScannerWorkerThread>();
		this.closed = true;
		initialize();
	}

	protected void initialize() throws IOException {
		try {
			this.iter = this.scans.iterator();
			if (this.iter.hasNext()) {
				this.currThread = new ParallelScannerWorkerThread(
						(Scan) this.iter.next());
				this.currThread.start();
			}
			while ((this.iter.hasNext())
					&& (this.nextThreads.size() < this.threadCount)) {
				ParallelScannerWorkerThread worker = new ParallelScannerWorkerThread(
						(Scan) this.iter.next());

				this.nextThreads.offer(worker);
				worker.start();
			}
			this.closed = false;
		} catch (IOException e) {
			close();
			throw e;
		}
	}

	public void close() {
		this.closed = true;
		if (this.currThread != null) {
			this.currThread.stop("Closed by user.");
		}
		for (ParallelScannerWorkerThread worker : this.nextThreads)
			worker.stop("Closed by user.");
	}

	public Result next() throws IOException {
		try {
			return nextInternal();
		} catch (IOException e) {
			close();
			throw e;
		}
	}

	private Result nextInternal() throws IOException {
		if ((this.closed) || (this.currThread == null)) {
			return null;
		}

		Result next = this.currThread.next();
		if (next != null) {
			return next;
		}

		if (this.currThread.isError()) {
			Exception ex = this.currThread.getException();
			throw ((ex instanceof IOException) ? (IOException) ex
					: new IOException(ex));
		}

		while ((next == null) && (this.currThread != null)) {
			if (!this.currThread.isStopped()) {
				this.currThread.stop("Scanner complete.");
			}
			this.currThread = ((ParallelScannerWorkerThread) this.nextThreads
					.poll());
			if (this.iter.hasNext()) {
				ParallelScannerWorkerThread worker = new ParallelScannerWorkerThread(
						(Scan) this.iter.next());

				this.nextThreads.offer(worker);
				worker.start();
			}
			if (this.currThread != null) {
				next = this.currThread.next();
			}
		}

		return next;
	}

	public Result[] next(int nbRows) throws IOException {
		ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
		for (int i = 0; i < nbRows; i++) {
			Result next = next();
			if (next == null)
				break;
			resultSets.add(next);
		}

		return (Result[]) resultSets.toArray(new Result[resultSets.size()]);
	}

	private void sort(List<Scan> scanList) throws IOException {
		Collections.sort(scanList, new ScanComparator());
		for (int i = 1; i < scanList.size(); i++) {
			byte[] currentStartRow = ((Scan) scanList.get(i)).getStartRow();
			byte[] lastStopRow = ((Scan) scanList.get(i - 1)).getStopRow();

			if ((lastStopRow == null) || (lastStopRow.length == 0))
				throw new IOException(
						"Scan has overlap, last scan's stopRow is null");
			if ((currentStartRow == null) || (currentStartRow.length == 0)) {
				throw new IOException(
						"Scan has overlap, current scan's startRow is null");
			}
			if (0 >= Bytes.compareTo(lastStopRow, 0, lastStopRow.length,
					currentStartRow, 0, currentStartRow.length))
				continue;
			throw new IOException(
					"Scan has overlap, current scan's startRow is smaller than last scan's stop row");
		}
	}

	private static class ScanComparator implements Comparator<Scan> {
		public int compare(Scan o1, Scan o2) {
			if ((o1.getStartRow() == null) || (o1.getStartRow().length == 0))
				return -1;
			if ((o2.getStartRow() == null) || (o2.getStartRow().length == 0)) {
				return 1;
			}
			byte[] o1StartRow = o1.getStartRow();
			byte[] o2StartRow = o2.getStartRow();
			return Bytes.compareTo(o1StartRow, 0, o1StartRow.length,
					o2StartRow, 0, o2StartRow.length);
		}
	}

	private class ParallelScannerWorkerThread extends Thread implements
			Stoppable {
		private ResultScanner scanner;
		private BlockingQueue<Result> results;
		private Object empty;
		private AtomicBoolean stopped;
		private Exception exception;

		protected ParallelScannerWorkerThread(Scan scan) throws IOException {
			this.scanner = ParallelClientScanner.this.table.getScanner(scan);
			this.results = new ArrayBlockingQueue<Result>(
					ParallelClientScanner.this.resultQueueSize);
			this.empty = new Object();
			this.stopped = new AtomicBoolean(false);
		}

		public Result next() throws IOException {
			Result r = (Result) this.results.poll();
			while ((!this.stopped.get()) && (r == null)) {
				try {
					synchronized (this.empty) {
						this.empty.wait();
					}
					r = (Result) this.results.poll();
				} catch (InterruptedException e) {
					throw ((IOException) (IOException) new IOException()
							.initCause(e));
				}
			}

			return r;
		}

		public void stop(String why) {
			this.stopped.compareAndSet(false, true);
		}

		public boolean isStopped() {
			return this.stopped.get();
		}

		public boolean isError() {
			return this.exception != null;
		}

		public Exception getException() {
			return this.exception;
		}

		public void run() {
			try {
				Result r = this.scanner.next();
				while (r != null) {
					boolean added = false;
					while (!added) {
						added = this.results.offer(r, SCANNER_WAIT_TIME,
								TimeUnit.MILLISECONDS);
					}
					synchronized (this.empty) {
						this.empty.notify();
					}
					r = this.scanner.next();
				}
			} catch (IOException ioe) {
				this.exception = ioe;
			} catch (InterruptedException ite) {
				this.exception = ite;
			} finally {
				this.scanner.close();
				this.stopped.compareAndSet(false, true);
				synchronized (this.empty) {
					this.empty.notify();
				}
			}
		}
	}
}