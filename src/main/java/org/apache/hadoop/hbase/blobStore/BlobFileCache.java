package org.apache.hadoop.hbase.blobStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.IdLock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class BlobFileCache {
	private static final Log LOG = LogFactory.getLog(BlobFileCache.class);

	private Map<String, CachedBlobFile> map = null;
	private final AtomicLong count;
	private final AtomicLong miss;
	private final ReentrantLock evictionLock = new ReentrantLock(true);

	private IdLock keyLock = new IdLock();

	private final ScheduledExecutorService scheduleThreadPool = Executors
			.newScheduledThreadPool(1, new ThreadFactoryBuilder()
					.setNameFormat("BlobFileCache #%d").setDaemon(true).build());
	private Configuration conf;
	private static final int EVICTION_CHECK_PERIOD = 3600;
	private static final int DEFAULT_MAX_BLOB_FILE_CACHE_SIZE = 1000;
	private int maxBlobFileCacheSize;
	private static final String MAX_CACHE_SIZE_PROPERTY = new StringBuilder()
			.append(BlobFileCache.class.getName()).append(".maxCacheSize")
			.toString();

	public BlobFileCache(Configuration conf) {
		this.conf = conf;
		this.maxBlobFileCacheSize = Integer.parseInt(conf.get(
				MAX_CACHE_SIZE_PROPERTY, String.valueOf(DEFAULT_MAX_BLOB_FILE_CACHE_SIZE)));

		this.map = new ConcurrentHashMap<String, CachedBlobFile>(this.maxBlobFileCacheSize);
		this.count = new AtomicLong(0L);
		this.miss = new AtomicLong(0L);
		LOG.info("BlobFileCache Initialize");
		this.scheduleThreadPool.scheduleAtFixedRate(new EvictionThread(this),
				EVICTION_CHECK_PERIOD, EVICTION_CHECK_PERIOD, TimeUnit.SECONDS);
	}

	public void evict() {
		evict(false);
	}

	public void evict(boolean evictAll) {
		printStatistics();
		if (!this.evictionLock.tryLock()) {
			return;
		}
		try {
			if (evictAll) {
				for (String fileName : this.map.keySet()) {
					CachedBlobFile file = (CachedBlobFile) this.map
							.remove(fileName);
					if (null != file)
						try {
							file.close();
						} catch (IOException e) {
							LOG.error(e.getMessage(), e);
						}
				}
			} else {
				if (this.map.size() <= this.maxBlobFileCacheSize)
					return;
				List<CachedBlobFile> files = new ArrayList<CachedBlobFile>(this.map.size());
				for (CachedBlobFile file : this.map.values()) {
					files.add(file);
				}

				Collections.sort(files);

				for (int i = this.maxBlobFileCacheSize; i < files.size(); i++) {
					String name = ((CachedBlobFile) files.get(i)).getName();
					CachedBlobFile file = (CachedBlobFile) this.map
							.remove(name);
					if (null == file)
						continue;
					try {
						file.close();
					} catch (IOException e) {
						LOG.error(e.getMessage(), e);
					}
				}
			}
		} finally {
			this.evictionLock.unlock();
		}
	}

	public BlobFile open(FileSystem fs, Path path, CacheConfig cacheConf)
			throws IOException {
		String fileName = path.getName();
		CachedBlobFile cached = (CachedBlobFile) this.map.get(path.getName());
		if (null == cached) {
			IdLock.Entry lockEntry = this.keyLock.getLockEntry(fileName
					.hashCode());
			try {
				cached = (CachedBlobFile) this.map.get(fileName);
				if (null == cached) {
					cached = CachedBlobFile.create(fs, path, this.conf,
							cacheConf);

					cached.open();
					this.map.put(fileName, cached);
				}
			} catch (IOException e) {
				LOG.error(
						new StringBuilder()
								.append("BlobFileCache, Exception happen during open ")
								.append(path.toString()).toString(), e);

				return null;
			} finally {
				this.miss.incrementAndGet();
				this.keyLock.releaseLockEntry(lockEntry);
			}
		}
		cached.open();
		cached.access(this.count.incrementAndGet());
		return cached;
	}

	public int getCacheSize() {
		if (this.map != null) {
			return this.map.size();
		}
		return 0;
	}

	public void printStatistics() {
		long access = this.count.get();
		long missed = this.miss.get();
		LOG.info(new StringBuilder()
				.append("BlobFileCache Statistics, access: ").append(access)
				.append(", miss: ").append(missed).append(", hit: ")
				.append(access - missed).append(", hit rate: ")
				.append(access == 0L ? 0L : (access - missed) * 100L / access)
				.append("%").toString());
	}

	static class EvictionThread extends Thread {
		BlobFileCache lru;

		public EvictionThread(BlobFileCache lru) {
			super();
			setDaemon(true);
			this.lru = lru;
		}

		public void run() {
			this.lru.evict();
		}
	}
}