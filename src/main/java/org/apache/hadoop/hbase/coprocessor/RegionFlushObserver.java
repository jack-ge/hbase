package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

public abstract interface RegionFlushObserver extends RegionObserver {
	public abstract void onFlush(
			ObserverContext<RegionCoprocessorEnvironment> paramObserverContext)
			throws IOException;
}