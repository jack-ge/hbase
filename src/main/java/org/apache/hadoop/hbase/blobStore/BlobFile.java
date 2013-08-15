package org.apache.hadoop.hbase.blobStore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;

public class BlobFile {
	private static final Log LOG = LogFactory.getLog(BlobFile.class);
	private StoreFile sf;

	protected BlobFile(StoreFile sf) {
		this.sf = sf;
	}

	public BlobFileScanner getScanner() throws IOException {
		List<StoreFile> sfContainer = new ArrayList<StoreFile>();
		sfContainer.add(this.sf);

		List<StoreFileScanner> sfScanners = StoreFileScanner.getScannersForStoreFiles(
				sfContainer, false, true, false, null);

		if ((null != sfScanners) && (sfScanners.size() > 0)) {
			BlobFileScanner scanner = new BlobFileScanner(
					(StoreFileScanner) sfScanners.get(0));
			return scanner;
		}
		return null;
	}

	public KeyValue readKeyValue(KeyValue search) throws IOException {
		KeyValue result = null;
		BlobFileScanner scanner = null;
		String msg = "";
		List<StoreFile> sfContainer = new ArrayList<StoreFile>();
		sfContainer.add(this.sf);
		try {
			List<StoreFileScanner> sfScanners = StoreFileScanner.getScannersForStoreFiles(
					sfContainer, false, true, false, null);

			if ((null != sfScanners) && (sfScanners.size() > 0)) {
				scanner = new BlobFileScanner(
						(StoreFileScanner) sfScanners.get(0));
				if (true == scanner.seek(search))
					result = scanner.peek();
			}
		} catch (IOException ioe) {
			msg = "Failed to ready key value! ";
			if ((ioe.getCause() instanceof FileNotFoundException)) {
				msg = msg + "The blob file does not exist!";
			}
			LOG.error(msg, ioe);
			result = null;
		} catch (NullPointerException npe) {
			msg = "Failed to ready key value! ";
			LOG.error(msg, npe);
			result = null;
		} finally {
			if (scanner != null) {
				scanner.close();
			}
		}
		return result;
	}

	public String getName() {
		return this.sf.getPath().getName();
	}

	public void open() throws IOException {
		if (this.sf.getReader() == null)
			this.sf.createReader();
	}

	public void close() throws IOException {
		if (null != this.sf) {
			this.sf.closeReader(false);
			this.sf = null;
		}
	}

	public static BlobFile create(FileSystem fs, Path path, Configuration conf,
			CacheConfig cacheConf) throws IOException {
		StoreFile sf = new StoreFile(fs, path, conf, cacheConf,
				StoreFile.BloomType.NONE, NoOpDataBlockEncoder.INSTANCE);

		return new BlobFile(sf);
	}
}