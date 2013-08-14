package org.apache.hadoop.hbase;

public abstract interface ProgressableCancellable {
	public abstract long getProgress();

	public abstract void cancel();
}