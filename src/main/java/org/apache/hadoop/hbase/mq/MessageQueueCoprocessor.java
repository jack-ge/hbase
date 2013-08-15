package org.apache.hadoop.hbase.mq;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactSelection;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class MessageQueueCoprocessor extends BaseRegionObserver {
	public void preCompactSelection(
			ObserverContext<RegionCoprocessorEnvironment> c, Store store,
			List<StoreFile> candidates) throws IOException {
		c.bypass();

		Configuration conf = ((RegionCoprocessorEnvironment) c.getEnvironment())
				.getConfiguration();

		long ttl = store.getFamily().getTimeToLive();
		if (ttl == 2147483647L) {
			ttl = 9223372036854775807L;
			return;
		}
		if (ttl == -1L) {
			ttl = 9223372036854775807L;
			return;
		}

		ttl *= 1000L;

		long minVersions = store.getFamily().getMinVersions();
		if ((conf.getBoolean("hbase.store.delete.expired.storefile", true))
				&& (ttl != 9223372036854775807L) && (minVersions == 0L)) {
			CompactSelection compactSelection = new CompactSelection(conf,
					candidates);

			CompactSelection expiredSelection = compactSelection
					.selectExpiredStoreFilesToCompact(EnvironmentEdgeManager
							.currentTimeMillis() - ttl);

			candidates.clear();

			if (expiredSelection != null)
				candidates.addAll(expiredSelection.getFilesToCompact());
		}
	}

	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
			Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		long time = System.currentTimeMillis();

		Map<byte[], List<KeyValue>> families = put.getFamilyMap();

		if (null == families) {
			return;
		}
		Collection<List<KeyValue>> kvSet = families.values();

		if ((null == kvSet) || (kvSet.size() == 0)) {
			return;
		}

		List<KeyValue> kvs = kvSet.iterator().next();
		if (kvs == null) {
			return;
		}

		byte[] row = put.getRow();
		Bytes.putLong(row, 2, time);

		for (int i = 0; i < kvs.size(); i++) {
			KeyValue kv = (KeyValue) kvs.get(i);
			byte[] buffer = kv.getBuffer();
			Bytes.putLong(buffer, kv.getRowOffset() + 2, time);
		}
	}
}