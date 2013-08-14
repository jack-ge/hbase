package org.apache.hadoop.hbase.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class MergeSortIterator<T> implements Iterator<T> {
	private PriorityQueue<IterWrapper<T>> heap = null;

	public MergeSortIterator(List<? extends Iterator<T>> iters,
			Comparator<T> comparator) {
		if ((null == iters) || (iters.size() == 0)) {
			throw new IllegalArgumentException(
					"Parameter is invalid, iterator list cannot be empty or null");
		}
		this.heap = new PriorityQueue<IterWrapper<T>>(iters.size());
		for (int i = 0; i < iters.size(); i++) {
			IterWrapper<T> wrapper = new IterWrapper<T>(iters.get(i),
					comparator);
			if (wrapper.next())
				this.heap.add(wrapper);
		}
	}

	public boolean hasNext() {
		return this.heap.size() != 0;
	}

	public T next() {
		IterWrapper<T> iter = this.heap.poll();
		if (null == iter) {
			return null;
		}
		T current = iter.getCurrent();
		if (iter.next()) {
			this.heap.add(iter);
		}
		return current;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	private static class IterWrapper<T> implements Comparable<IterWrapper<T>> {
		private T current = null;
		private Iterator<T> iter;
		private Comparator<T> comparator;

		public IterWrapper(Iterator<T> iter, Comparator<T> comparator) {
			this.iter = iter;
			this.comparator = comparator;
		}

		public int compareTo(IterWrapper<T> other) {
			T my = getCurrent();
			T they = other.getCurrent();
			return this.comparator.compare(my, they);
		}

		public T getCurrent() {
			return this.current;
		}

		public boolean next() {
			if (this.iter.hasNext()) {
				this.current = this.iter.next();
				return null != this.current;
			}
			return false;
		}
	}
}