package org.apache.hadoop.hbase.blobStore;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.util.ThreadSafeSimpleDateFormat;

public class BlobStoreUtils {
	private static final ThreadSafeSimpleDateFormat formatter = new ThreadSafeSimpleDateFormat(
			"yyyyMMdd");

	public static Path getTableBlobStorePath(Path rootdir, String tableName)
			throws IOException {
		Path resultPath = null;
		if (rootdir != null) {
			resultPath = new Path(rootdir, "blobstore/" + tableName);
		}

		return resultPath;
	}

	public static void cleanObsoleteData(FileSystem fs, BlobStore store)
			throws IOException {
		cleanObsoleteDate(fs, store, new Date());
	}

	public static void cleanObsoleteDate(FileSystem fs, BlobStore store,
			Date current) throws IOException {
		HColumnDescriptor columnDescriptor = store.getColumnDescriptor();

		if (current == null) {
			current = new Date();
		}

		long timeToLive = columnDescriptor.getTimeToLive();

		if (2147483647L == timeToLive) {
			return;
		}

		Date obsolete = new Date(current.getTime() - timeToLive * 1000L);

		FileStatus[] stats = fs.listStatus(store.getHomePath());
		if (null == stats) {
			return;
		}
		for (int i = 0; i < stats.length; i++) {
			FileStatus file = stats[i];
			if (!file.isDir()) {
				continue;
			}
			String fileName = file.getPath().getName();
			try {
				Date fileDate = formatter.parse(fileName);
				if (fileDate.getTime() < obsolete.getTime())
					fs.delete(file.getPath(), true);
			} catch (ParseException e) {
				new Exception("Cannot parse the fileName as date" + fileName, e)
						.printStackTrace();
			}
		}
	}
}