package org.apache.hadoop.hbase.regionserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import org.apache.commons.collections.collection.CompositeCollection;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.hbase.client.Scan;

public class FirstKeySortedStoreFiles implements ChangedReadersObserver,
		Closeable {
	final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private Comparator<StoreFile> comparator;
	private Store store;
	private List<StoreFile> storeFiles;
	private boolean closing;
	private IntervalTree intervalTree;

	public FirstKeySortedStoreFiles(Store store) {
		this.comparator = new FirstKeyComparator();
		this.store = store;

		this.storeFiles = sortAndClone(store.getStorefiles());
		this.intervalTree = new IntervalTree(this.storeFiles);
		store.addChangedReaderObserver(this);
	}

	public Collection<StoreFile> getStoreFiles(Scan scan) {
		this.lock.readLock().lock();
		IntervalTree intervalTree = this.intervalTree;
		List storeFiles = this.storeFiles;
		this.lock.readLock().unlock();

		if ((null == scan) || (scan.getStartRow().length == 0)) {
			return storeFiles;
		}
		CompositeCollection collections = new CompositeCollection();

		byte[] search = KeyValue.createFirstOnRow(scan.getStartRow()).getKey();

		List intsected = intervalTree.findIntesection(search);
		if ((null != intsected) && (intsected.size() != 0)) {
			collections.addComposited(intsected);
		}
		int index = binarySearch(storeFiles, search);
		if (-1 != index) {
			List subList = storeFiles.subList(index, storeFiles.size());
			collections.addComposited(subList);
		}
		return collections;
	}

	private int binarySearch(List<StoreFile> storeFiles, byte[] search) {
		if ((null == storeFiles) || (storeFiles.size() == 0)) {
			return -1;
		}
		int low = 0;
		int high = storeFiles.size() - 1;
		int result = -1;
		while (low <= high) {
			int mid = low + high >>> 1;
			StoreFile midVal = (StoreFile) storeFiles.get(mid);
			int cmp = KeyValue.KEY_COMPARATOR.compare(midVal.getReader()
					.getFirstKey(), search);

			if (cmp < 0) {
				low = mid + 1;
			} else if (cmp > 0) {
				high = mid - 1;
			} else {
				result = mid + 1;
				break;
			}
		}
		if (result == -1) {
			result = low + 1;
		}
		return result >= storeFiles.size() ? -1 : result;
	}

	public long getMaxSequenceId() {
		return this.store.getMaxSequenceId();
	}

	private List<StoreFile> sortAndClone(List<StoreFile> input) {
		List storeFiles = new ArrayList();
		for (StoreFile f : input) {
			if (null == f.getReader()) {
				continue;
			}
			byte[] firstKey = f.getReader().getFirstKey();
			if ((null == firstKey) || (firstKey.length == 0)) {
				continue;
			}
			storeFiles.add(f);
		}
		Collections.sort(storeFiles, this.comparator);
		return storeFiles;
	}

	public void updateReaders() throws IOException {
		this.lock.writeLock().lock();
		try {
			this.storeFiles = sortAndClone(this.store.getStorefiles());
			this.intervalTree = new IntervalTree(this.storeFiles);
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	public void close() throws IOException {
		this.lock.writeLock().lock();
		try {
			if (!this.closing) {
				this.closing = true;
				if (null != this.store)
					this.store.deleteChangedReaderObserver(this);
			}
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	private class IntervalTree {
		private List<FirstKeySortedStoreFiles.TreeNode> nodes;

		IntervalTree(List<StoreFile> storeFiles) {
			buildTree(storeFiles);
		}

		private void buildTree(List<StoreFile> files) {
			int depth = getDepth(files.size());
			int length = 1 << depth;
			this.nodes = new ArrayList(length);
			for (int i = 0; i < length; i++) {
				this.nodes.add(null);
			}
			buildTree(files, 0, files.size(), 0, this.nodes);
			updateMaxBoundary(this.nodes);
		}

		private void updateMaxBoundary(
				List<FirstKeySortedStoreFiles.TreeNode> nodes) {
			for (int i = nodes.size() - 1; i >= 0; i--) {
				FirstKeySortedStoreFiles.TreeNode node = (FirstKeySortedStoreFiles.TreeNode) nodes
						.get(i);
				if (null == node) {
					continue;
				}
				int parentIndex = (i - 1) / 2;
				if (parentIndex <= 0) {
					continue;
				}
				FirstKeySortedStoreFiles.TreeNode parent = (FirstKeySortedStoreFiles.TreeNode) nodes
						.get(parentIndex);
				parent.maxKey = max(parent.maxKey, node.maxKey);
			}
		}

		public List<StoreFile> findIntesection(byte[] key) {
			if ((null == key) || (key.length == 0)) {
				return null;
			}
			List results = new ArrayList();
			findIntesection(key, results, 0);
			Collections.sort(results, FirstKeySortedStoreFiles.this.comparator);
			return results;
		}

		private void findIntesection(byte[] key, List<StoreFile> result,
				int index) {
			if (index >= this.nodes.size()) {
				return;
			}

			FirstKeySortedStoreFiles.TreeNode node = (FirstKeySortedStoreFiles.TreeNode) this.nodes
					.get(index);
			if (null == node) {
				return;
			}

			if (compareKey(key, node.maxKey) > 0) {
				return;
			}

			if (compareKey(key, node.file.getReader().getFirstKey()) < 0) {
				findIntesection(key, result, 2 * index + 1);
				return;
			}

			if (compareKey(key, node.file.getReader().getLastKey()) <= 0) {
				result.add(node.file);
			}

			findIntesection(key, result, 2 * index + 1);
			findIntesection(key, result, 2 * index + 2);
		}

		private int compareKey(byte[] key1, byte[] key2) {
			return KeyValue.KEY_COMPARATOR.compare(key1, key2);
		}

		private byte[] max(byte[] key1, byte[] key2) {
			if (null == key1) {
				return key2;
			}
			if (null == key2) {
				return key1;
			}
			return KeyValue.KEY_COMPARATOR.compare(key1, key2) >= 0 ? key1
					: key2;
		}

		private int getDepth(int size) {
			int depth = 0;
			while (0 != size) {
				depth++;
				size >>= 1;
			}
			return depth;
		}

		private void buildTree(List<StoreFile> data, int start, int end,
				int position, List<FirstKeySortedStoreFiles.TreeNode> result) {
			if (end <= start) {
				return;
			}
			int middle = (start + end) / 2;
			result.set(position, new FirstKeySortedStoreFiles.TreeNode(
					(StoreFile) data.get(middle)));
			int leftChildPosition = 2 * position + 1;
			int rightChildPosition = 2 * position + 2;

			buildTree(data, start, middle, leftChildPosition, result);
			buildTree(data, middle + 1, end, rightChildPosition, result);
		}
	}

	private static class TreeNode {
		StoreFile file;
		byte[] maxKey;

		public TreeNode(StoreFile storeFile) {
			this.file = storeFile;
			this.maxKey = this.file.getReader().getLastKey();
		}
	}

	public static class FirstKeyComparator implements Comparator<StoreFile> {
		private KeyValue.KeyComparator keyComparator = KeyValue.KEY_COMPARATOR;

		public int compare(StoreFile o1, StoreFile o2) {
			StoreFile.Reader o1Reader = o1.getReader();
			if (null == o1Reader) {
				return -1;
			}
			byte[] k1 = o1.getReader().getFirstKey();
			if (null == k1) {
				return -1;
			}

			StoreFile.Reader o2Reader = o2.getReader();
			if (null == o2Reader) {
				return -1;
			}
			byte[] k2 = o2.getReader().getFirstKey();
			if (null == k2) {
				return -1;
			}

			return this.keyComparator.compare(k1, k2);
		}
	}
}