package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;

public class RollingStoreFileScanner implements KeyValueScanner,
		ChangedReadersObserver {
	static final Log LOG = LogFactory.getLog(Store.class);
	private boolean seekDone = false;
	private long sequenceId;
	private RollingHeap heap;
	private boolean closing;
	private Store store;
	private FirstKeySortedStoreFiles firstKeySortedStoreFiles;
	private KeyValue.KVComparator kvComparator;
	private boolean updated;
	private Scan scan;

	public RollingStoreFileScanner(Store store,
			FirstKeySortedStoreFiles firstKeySortedStoreFiles,
			NavigableSet<byte[]> targetCols, Scan scan) throws IOException {
		this.store = store;
		this.scan = scan;
		this.firstKeySortedStoreFiles = firstKeySortedStoreFiles;
		Collection<StoreFile> storeFiles = firstKeySortedStoreFiles.getStoreFiles(scan);
		this.heap = new RollingHeap(storeFiles.iterator(),
				new KeyValue.KVComparator());
		this.sequenceId = firstKeySortedStoreFiles.getMaxSequenceId();

		this.kvComparator = new KeyValue.KVComparator();
		store.addChangedReaderObserver(this);
	}

	public synchronized KeyValue peek() {
		return this.heap.peek();
	}

	public synchronized KeyValue next() throws IOException {
		checkUpdate();
		return this.heap.poll();
	}

	public synchronized boolean reseek(KeyValue key) throws IOException {
		return seek(key);
	}

	public long getSequenceID() {
		return this.sequenceId;
	}

	public synchronized void close() {
		if (this.closing)
			return;
		this.closing = true;
		this.heap.close();
		if (this.store != null)
			this.store.deleteChangedReaderObserver(this);
	}

	public synchronized boolean seek(KeyValue key) throws IOException {
		checkUpdate();
		this.seekDone = true;
		return this.heap.seek(key);
	}

	public synchronized boolean requestSeek(KeyValue kv, boolean forward,
			boolean useBloom) throws IOException {
		return seek(kv);
	}

	public boolean realSeekDone() {
		return this.seekDone;
	}

	public void enforceSeek() throws IOException {
	}

	public boolean isFileScanner() {
		return true;
	}

	public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns,
			long oldestUnexpiredTS) {
		return true;
	}

	public synchronized void updateReaders() throws IOException {
		if (this.closing) {
			return;
		}
		this.sequenceId = this.firstKeySortedStoreFiles.getMaxSequenceId();
		this.updated = true;
	}

	private void checkUpdate() throws IOException {
		if (this.updated) {
			this.updated = false;

			Collection<StoreFile> newStoreFiles = null;

			this.seekDone = false;
			KeyValue lastTop = this.heap.peek();
			if (null == lastTop) {
				newStoreFiles = this.firstKeySortedStoreFiles
						.getStoreFiles(this.scan);
			} else {
				Scan newScan = new Scan(this.scan);
				newScan.setStartRow(lastTop.getRow());
				newStoreFiles = this.firstKeySortedStoreFiles
						.getStoreFiles(newScan);
			}
			this.heap.close();
			this.heap = new RollingHeap(newStoreFiles.iterator(),
					this.kvComparator);
			if (null != lastTop) {
				this.heap.seek(lastTop);
			}
			this.seekDone = true;
		}
	}

	private static class RollingBar {
		byte[] key;
		KeyValue kv;
		private static final KeyValue MIN_KEYVALUE;
		private static final KeyValue MAX_KEYVALUE;
		public static RollingBar MIN;
		public static RollingBar MAX;

		public RollingBar(KeyValue kv) {
			this.kv = kv;
		}

		public void set(byte[] key) {
			this.key = key;
			this.kv = null;
		}

		public KeyValue get() {
			if (null == this.kv) {
				this.kv = KeyValue.createKeyValueFromKey(this.key);
			}
			return this.kv;
		}

		public int compareTo(KeyValue kv) {
			if (this == MIN) {
				return -1;
			}
			if (this == MAX) {
				return 1;
			}
			return KeyValue.COMPARATOR.compare(get(), kv);
		}

		static {
			byte[] min = new byte[0];
			MIN_KEYVALUE = KeyValue.createFirstOnRow(min);

			int limit = 1024;
			byte[] max = new byte[limit];
			Arrays.fill(max, (byte)-1);
			MAX_KEYVALUE = KeyValue.createFirstOnRow(max);

			MIN = new RollingBar(MIN_KEYVALUE);
			MAX = new RollingBar(MAX_KEYVALUE);
		}
	}

	public static class RollingHeap {
		private PriorityQueue<KeyValueScanner> heap = null;
		private final int initialHeapSize = 1;

		private RollingStoreFileScanner.RollingBar bar = null;
		private Iterator<StoreFile> files;
		private KeyValue.KVComparator kvComparator;
		private boolean closed;

		public RollingHeap(Iterator<StoreFile> storeFiles,
				KeyValue.KVComparator comparator) throws IOException {
			KeyValueHeap.KVScannerComparator scannerComparator = new KeyValueHeap.KVScannerComparator(
					comparator);
			this.kvComparator = comparator;

			this.files = storeFiles;

			this.bar = RollingStoreFileScanner.RollingBar.MIN;

			this.heap = new PriorityQueue<KeyValueScanner>(initialHeapSize, scannerComparator);
		}

		public KeyValue peek() {
			if (this.closed) {
				return null;
			}
			return peek(this.heap);
		}

		public KeyValue poll() throws IOException {
			if (this.closed) {
				return null;
			}
			KeyValueScanner scanner = (KeyValueScanner) this.heap.poll();
			if (null == scanner) {
				return null;
			}
			KeyValue current = scanner.next();
			if (null == scanner.peek()) {
				scanner.close();
			} else {
				this.heap.add(scanner);
			}
			if (needRolling(this.heap, this.bar)) {
				seek(this.bar.get());
			}
			return current;
		}

		private int compare(KeyValue kv1, KeyValue kv2) {
			return this.kvComparator.compare(kv1, kv2);
		}

		public void close() {
			if (!this.closed) {
				KeyValueScanner scanner;
				while ((scanner = (KeyValueScanner) this.heap.poll()) != null) {
					scanner.close();
				}
				this.closed = true;
			}
		}

		public boolean seek(KeyValue seek) throws IOException {
			if (this.closed) {
				return false;
			}
			seek(this.heap, seek);

			if (!needRolling(this.heap, this.bar)) {
				return true;
			}

			while (this.files.hasNext()) {
				StoreFile file = (StoreFile) this.files.next();
				byte[] firstKey = getFirstKey(file);
				if (null == firstKey) {
					continue;
				}
				this.bar.set(firstKey);

				if (!needRolling(this.heap, this.bar)) {
					break;
				}
				byte[] lastKey = getLastKey(file);
				if ((null == lastKey) || (compareKey(seek, lastKey) > 0)) {
					continue;
				}
				StoreFileScanner scanner = getScanner(file);
				boolean found = scanner.seek(seek);
				if (!found)
					scanner.close();
				else {
					this.heap.add(scanner);
				}
			}

			if (!this.files.hasNext()) {
				this.bar = RollingStoreFileScanner.RollingBar.MAX;
			}
			return null != this.heap.peek();
		}

		private int compareKey(KeyValue kv1, byte[] k2) {
			int ret = this.kvComparator.getRawComparator().compare(
					kv1.getBuffer(), kv1.getOffset() + 8, kv1.getKeyLength(),
					k2, 0, k2.length);

			return ret;
		}

		private boolean needRolling(PriorityQueue<KeyValueScanner> heap,
				RollingStoreFileScanner.RollingBar bar) {
			if (null == heap) {
				return true;
			}
			KeyValue heapTop = peek(heap);

			return (null == heapTop) || (bar.compareTo(heapTop) <= 0);
		}

		private byte[] getFirstKey(StoreFile file) {
			if (null == file) {
				return null;
			}
			return file.getReader().getFirstKey();
		}

		private byte[] getLastKey(StoreFile file) {
			if (null == file) {
				return null;
			}
			StoreFile.Reader fileReader = file.getReader();
			if (null == fileReader) {
				return null;
			}
			if (null == fileReader.getLastKey()) {
				return null;
			}
			return fileReader.getLastKey();
		}

		private StoreFileScanner getScanner(StoreFile file) {
			StoreFile.Reader reader = file.getReader();
			StoreFileScanner scanner = reader.getStoreFileScanner(false, true);
			return scanner;
		}

		private KeyValue peek(PriorityQueue<KeyValueScanner> heap) {
			if (null == heap) {
				return null;
			}
			if (null == heap.peek()) {
				return null;
			}
			return ((KeyValueScanner) heap.peek()).peek();
		}

		private boolean seek(PriorityQueue<KeyValueScanner> heap, KeyValue key)
				throws IOException {
			if ((null == heap) || (null == heap.peek())) {
				return false;
			}

			KeyValue top = ((KeyValueScanner) heap.peek()).peek();
			if (compare(key, top) <= 0) {
				return true;
			}

			KeyValueScanner scanner = null;
			while (null != (scanner = (KeyValueScanner) heap.poll())) {
				KeyValue current = scanner.peek();
				if (null == current) {
					scanner.close();
					continue;
				}

				if (compare(key, current) <= 0) {
					heap.add(scanner);
					return true;
				}

				boolean found = scanner.seek(key);
				if (!found)
					scanner.close();
				else {
					heap.add(scanner);
				}
			}

			return false;
		}
	}
}