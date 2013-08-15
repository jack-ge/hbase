package org.apache.hadoop.hbase.blobStore;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

public class CachedBlobFile extends BlobFile implements
		Comparable<CachedBlobFile> {
	private long accessTime;
	private BlobFile file;
	private AtomicLong reference = new AtomicLong(0L);

	public CachedBlobFile(BlobFile file) {
		super(null);
		this.file = file;
	}

	public void access(long time) {
		this.accessTime = time;
	}

	public int compareTo(CachedBlobFile that) {
		if (this.accessTime == that.accessTime)
			return 0;
		return this.accessTime < that.accessTime ? 1 : -1;
	}

	public void open() throws IOException {
		this.file.open();
		this.reference.incrementAndGet();
	}

	public KeyValue readKeyValue(KeyValue search) throws IOException {
		return this.file.readKeyValue(search);
	}

	public String getName() {
		return this.file.getName();
	}

	public void close() throws IOException {
		long refs = this.reference.decrementAndGet();
		if (refs == 0L)
			this.file.close();
	}

	public long getReference() {
		return this.reference.get();
	}

	public static CachedBlobFile create(FileSystem fs, Path path,
			Configuration conf, CacheConfig cacheConf) throws IOException {
		BlobFile file = BlobFile.create(fs, path, conf, cacheConf);
		return new CachedBlobFile(file);
	}
}