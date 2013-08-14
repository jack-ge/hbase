package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

public abstract interface WALRestoreFlushObserver extends RegionObserver {
	public abstract void preFlushWALRestore(
			ObserverContext<RegionCoprocessorEnvironment> paramObserverContext)
			throws IOException;

	public abstract void postFlushWALRestore(
			ObserverContext<RegionCoprocessorEnvironment> paramObserverContext)
			throws IOException;
}