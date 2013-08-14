package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.MergeSortIterator;

public class MergeSortScanner implements ResultScanner {
	private List<ScannerIterator> iters;
	private MergeSortIterator<Result> iter;
	private boolean closed = false;

	public MergeSortScanner(Scan[] scans, HTableInterface table,
			int commonPrefixLength) throws IOException {
		this.iters = new ArrayList();

		for (int i = 0; i < scans.length; i++) {
			ResultScanner scanner = table.getScanner(scans[i]);
			this.iters.add(new ScannerIterator(scanner));
		}
		this.iter = new MergeSortIterator(this.iters,
				new IgnorePrefixComparator(commonPrefixLength));
	}

	public Result next() throws IOException {
		if (this.closed)
			return null;
		try {
			return (Result) this.iter.next();
		} catch (Throwable t) {
			throw new IOException(t);

		}
	}

	public Result[] next(int nbRows) throws IOException {
		ArrayList resultSets = new ArrayList(nbRows);
		for (int i = 0; i < nbRows; i++) {
			Result next = next();
			if (next == null)
				break;
			resultSets.add(next);
		}

		return (Result[]) resultSets.toArray(new Result[resultSets.size()]);
	}

	public void close() {
		if (this.closed) {
			return;
		}
		for (ScannerIterator it : this.iters) {
			if (!it.isClosed()) {
				it.close();
			}
		}
		this.closed = true;
	}

	public Iterator<Result> iterator() {
		return this.iter;
	}

	private class ScannerIterator implements Iterator<Result> {
		private ResultScanner scanner;
		private Result next = null;
		private boolean closed = false;

		protected ScannerIterator(ResultScanner scanner) {
			this.scanner = scanner;
		}

		public boolean isClosed() {
			return this.closed;
		}

		public void close() {
			if (this.closed)
				return;
			this.scanner.close();
			this.closed = true;
		}

		public boolean hasNext() {
			if (this.next == null) {
				try {
					this.next = this.scanner.next();
					if (this.next == null) {
						close();
					}
					return this.next != null;
				} catch (IOException e) {
					MergeSortScanner.this.close();
					throw new RuntimeException(e);
				}
			}
			return true;
		}

		public Result next() {
			if (!hasNext()) {
				return null;
			}

			Result temp = this.next;
			this.next = null;
			return temp;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	private static class IgnorePrefixComparator implements Comparator<Result> {
		private static final KeyValue.KeyComparator comparator = new KeyValue.KeyComparator();
		private int prefixLength;

		public IgnorePrefixComparator(int prefixLength) {
			this.prefixLength = prefixLength;
		}

		public int compare(Result r1, Result r2) {
			if ((null == r1) || (null == r2)) {
				throw new IllegalArgumentException();
			}
			return comparator.compareIgnoringPrefix(this.prefixLength,
					r1.getRow(), 0, r1.getRow().length, r2.getRow(), 0,
					r2.getRow().length);
		}
	}
}