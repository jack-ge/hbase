package org.apache.hadoop.hbase.blobStore.compactions;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.blobStore.BlobStore;
import org.apache.hadoop.hbase.blobStore.BlobStoreManager;
import org.apache.hadoop.hbase.blobStore.BlobStoreManager.BlobStoreKey;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Sweeper extends Configured implements Tool {
	private HBaseAdmin admin;
	private Set<String> existingTables = new HashSet<String>();
	private FileSystem fs;

	public void init() throws IOException {
		Configuration conf = getConf();
		HBaseAdmin.checkHBaseAvailable(conf);
		this.admin = new HBaseAdmin(conf);
		this.fs = FileSystem.get(conf);
		Path hbaseDir = new Path(conf.get(HConstants.HBASE_DIR));
		Path rootDir = new Path(hbaseDir, HConstants.BLOB_STORE);
		BlobStoreManager.getInstance().init(this.fs, rootDir);
	}

	public void sweepAll() throws IOException, InterruptedException,
			ClassNotFoundException {
		List<BlobStoreKey> keys = BlobStoreManager.getInstance()
				.getAllBlobStores();
		if (null != keys)
			for (int i = 0; i < keys.size(); i++) {
				BlobStoreManager.BlobStoreKey key = keys.get(i);
				sweepFamily(key.getTableName(), key.getFamilyName());
			}
	}

	public void sweepTable(String tableName) throws IOException,
			InterruptedException, ClassNotFoundException {
		List<BlobStoreKey> keys = BlobStoreManager.getInstance().getBlobStores(
				tableName);

		if (null != keys)
			for (int i = 0; i < keys.size(); i++) {
				BlobStoreManager.BlobStoreKey key = keys.get(i);
				sweepFamily(key.getTableName(), key.getFamilyName());
			}
	}

	public void sweepFamily(String table, String family) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (!this.existingTables.contains(table)) {
			if (!this.admin.tableExists(table)) {
				throw new IOException("Table " + table + " not exist");
			}
			this.existingTables.add(table);
		}

		BlobStore store = BlobStoreManager.getInstance().getBlobStore(table,
				family);

		SweepJob job = new SweepJob(this.fs);
		job.sweep(store, getConf());
	}

	public static void main(String[] args) throws Exception {
		System.out.print("Usage:\n--------------------------\n"
				+ Sweeper.class.getName() + "[tableName] [familyName]");

		Configuration conf = getDefaultConfiguration(null);
		ToolRunner.run(conf, new Sweeper(), args);
	}

	private static Configuration getDefaultConfiguration(Configuration conf) {
		return conf == null ? HBaseConfiguration.create() : HBaseConfiguration
				.create(conf);
	}

	public int run(String[] args) throws Exception {
		init();
		if (args.length >= 2) {
			String table = args[0];
			String family = args[1];
			sweepFamily(table, family);
		} else if (args.length >= 1) {
			String table = args[0];
			sweepTable(table);
		} else {
			sweepAll();
		}
		return 0;
	}
}