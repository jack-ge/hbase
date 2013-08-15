package org.apache.hadoop.hbase.blobStore;

import java.io.IOException;
import java.util.SortedSet;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;

public class BlobFileScanner implements KeyValueScanner {
	private StoreFileScanner scanner;

	public BlobFileScanner(StoreFileScanner scanner) {
		this.scanner = scanner;
	}

	public KeyValue next() throws IOException {
		return this.scanner.next();
	}

	public void close() {
		this.scanner.close();
	}

	public KeyValue peek() {
		return this.scanner.peek();
	}

	public boolean seek(KeyValue key) throws IOException {
		return this.scanner.seek(key);
	}

	public boolean reseek(KeyValue key) throws IOException {
		return this.scanner.seek(key);
	}

	public long getSequenceID() {
		return this.scanner.getSequenceID();
	}

	public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns,
			long oldestUnexpiredTS) {
		return true;
	}

	public boolean requestSeek(KeyValue kv, boolean forward, boolean useBloom)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	public boolean realSeekDone() {
		throw new UnsupportedOperationException();
	}

	public void enforceSeek() throws IOException {
		throw new UnsupportedOperationException();
	}

	public boolean isFileScanner() {
		return true;
	}
}