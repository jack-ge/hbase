package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import org.apache.hadoop.hbase.regionserver.HRegion;

public abstract interface SplittableRegionObserver extends RegionObserver {
	public abstract void onSplit(
			ObserverContext<RegionCoprocessorEnvironment> paramObserverContext,
			HRegion paramHRegion1, HRegion paramHRegion2) throws IOException;
}