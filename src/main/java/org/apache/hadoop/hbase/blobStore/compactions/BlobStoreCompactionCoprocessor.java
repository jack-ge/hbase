package org.apache.hadoop.hbase.blobStore.compactions;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.blobStore.BlobStore;
import org.apache.hadoop.hbase.blobStore.BlobStoreManager;
import org.apache.hadoop.hbase.blobStore.BlobStoreUtils;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;

public class BlobStoreCompactionCoprocessor extends BaseRegionObserver {
	private long nextCheckTime = System.currentTimeMillis();

	public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,
			Store store, StoreFile resultFile) throws IOException {
		if (store.getFamily().isLobStoreEnabled()) {
			Configuration conf = ((RegionCoprocessorEnvironment) e
					.getEnvironment()).getConfiguration();

			long ttl = store.getFamily().getTimeToLive();

			long majorCompactionInterval = conf.getLong(
					HConstants.MAJOR_COMPACTION_PERIOD, 86400000L);

			if (store.getFamily().getValue(HConstants.MAJOR_COMPACTION_PERIOD) != null) {
				String strCompactionTime = store.getFamily().getValue(
						HConstants.MAJOR_COMPACTION_PERIOD);

				majorCompactionInterval = new Long(strCompactionTime)
						.longValue();
			}

			long currentTime = System.currentTimeMillis();
			if (currentTime < this.nextCheckTime) {
				return;
			}

			if (ttl == 2147483647L) {
				ttl = 9223372036854775807L;
			} else if (ttl == -1L) {
				ttl = 9223372036854775807L;
			} else {
				ttl *= 1000L;
			}

			long minVersions = store.getFamily().getMinVersions();
			if ((conf.getBoolean("hbase.store.delete.expired.storefile", true))
					&& (ttl != 9223372036854775807L) && (minVersions == 0L)) {
				FileSystem fs = FileSystem.get(conf);

				BlobStore blobStore = BlobStoreManager.getInstance()
						.getBlobStore(store.getTableName(),
								store.getFamily().getNameAsString());

				BlobStoreUtils.cleanObsoleteData(fs, blobStore);
			}

			this.nextCheckTime = (currentTime + majorCompactionInterval);
		}
	}
}