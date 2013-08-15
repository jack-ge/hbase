package org.apache.hadoop.hbase.blobStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;

public class BlobStoreManager {
	private Map<BlobStoreKey, BlobStore> stores = new ConcurrentHashMap<BlobStoreKey, BlobStore>();
	private Path homeDir;
	private FileSystem fs;
	private AtomicLong nextCleanCheckTime;
	public static final Log LOG = LogFactory.getLog(BlobStoreManager.class);
	private static final long CLEAN_CHECK_PERIOD = 86400000L;
	private static BlobStoreManager manager = new BlobStoreManager();

	public void init(FileSystem fs, Path homeDir) throws IOException {
		this.fs = fs;
		this.homeDir = homeDir;
		this.nextCleanCheckTime = new AtomicLong(
				System.currentTimeMillis() + CLEAN_CHECK_PERIOD);
		loadTables(homeDir);
	}

	private void loadTables(Path homeDir) throws IOException {
		FileStatus[] files = this.fs.listStatus(homeDir);
		if (null == files) {
			return;
		}
		for (int i = 0; i < files.length; i++)
			if (files[i].isDir())
				loadTable(homeDir, files[i].getPath());
	}

	private void loadTable(Path homeDir, Path path) throws IOException {
		FileStatus[] files = this.fs.listStatus(path);
		String tableName = path.getName();

		for (int i = 0; i < files.length; i++)
			if (files[i].isDir()) {
				Path familyPath = files[i].getPath();
				BlobStoreKey key = new BlobStoreKey(tableName,
						familyPath.getName());
				BlobStore blob = BlobStore.load(this.fs, homeDir, tableName,
						familyPath.getName());

				if (null != blob)
					this.stores.put(key, blob);
			}
	}

	public BlobStore getBlobStore(String tableName, String family) {
		BlobStoreKey key = new BlobStoreKey(tableName, family);
		return getBlobStore(key);
	}

	public BlobStore getBlobStore(BlobStoreKey key) {
		return (BlobStore) this.stores.get(key);
	}

	public List<BlobStoreKey> getBlobStores(String tableName) {
		Set<BlobStoreKey> keys = this.stores.keySet();
		Iterator<BlobStoreKey> iter = keys.iterator();
		if (null == iter) {
			return null;
		}
		List<BlobStoreKey> results = new ArrayList<BlobStoreKey>();
		while (iter.hasNext()) {
			BlobStoreKey key = (BlobStoreKey) iter.next();
			if ((null != key) && (key.getTableName().equals(tableName))) {
				results.add(key);
			}
		}
		return results;
	}

	public List<BlobStoreKey> getAllBlobStores() {
		Set<BlobStoreKey> keys = this.stores.keySet();
		Iterator<BlobStoreKey> iter = keys.iterator();
		if (null == iter) {
			return null;
		}
		List<BlobStoreKey> results = new ArrayList<BlobStoreKey>();
		while (iter.hasNext()) {
			BlobStoreKey key = (BlobStoreKey) iter.next();
			results.add(key);
		}
		return results;
	}

	public boolean removeBlobStores(List<BlobStoreKey> keyList)
			throws IOException {
		if (null == keyList) {
			return true;
		}
		for (BlobStoreKey key : keyList) {
			if (this.stores.containsKey(key)) {
				this.stores.remove(key);
			}
		}
		return true;
	}

	public boolean removeBlobStore(String tableName, String familyName)
			throws IOException {
		BlobStoreKey key = new BlobStoreKey(tableName, familyName);
		if (this.stores.containsKey(key)) {
			this.stores.remove(key);
		}
		return true;
	}

	public Path getHomeDir() {
		return this.homeDir;
	}

	public BlobStore createBlobStore(String tableName, HColumnDescriptor family)
			throws IOException {
		cleanBlobStoresIfNecessary();
		BlobStoreKey key = new BlobStoreKey(tableName, family.getNameAsString());
		Path homePath = new Path(this.homeDir, tableName + "/"
				+ family.getNameAsString());

		if (!this.stores.containsKey(key)) {
			BlobStore blobStore = BlobStore.load(this.fs, this.homeDir,
					tableName, family.getNameAsString());

			if (null == blobStore) {
				this.fs.mkdirs(homePath);
				blobStore = BlobStore.create(this.fs, homePath, family);
			}
			this.stores.put(key, blobStore);
		}
		return (BlobStore) this.stores.get(key);
	}

	private void cleanBlobStoresIfNecessary() {
		long currentTime = System.currentTimeMillis();
		if (currentTime < this.nextCleanCheckTime.get()) {
			return;
		}

		cleanObsolete();
		this.nextCleanCheckTime.set(currentTime + 86400000L);
	}

	private void cleanObsolete() {
		TableDescriptors tableDescriptors = new FSTableDescriptors(manager.fs,
				manager.homeDir.getParent());
		Map<String, HTableDescriptor> hTableDescriptors;

		try {
			hTableDescriptors = tableDescriptors.getAll();

			if ((this.stores != null) && (this.stores.size() > 0))
				for (BlobStoreKey blobStoreKey : this.stores.keySet()) {
					String tableName = blobStoreKey.getTableName();

					if (hTableDescriptors.containsKey(tableName)) {
						String familyName = blobStoreKey.getFamilyName();
						HTableDescriptor table = (HTableDescriptor) hTableDescriptors
								.get(tableName);
						Set<byte[]> familiesKeys = table.getFamiliesKeys();

						if (familiesKeys.contains(Bytes.toBytes(familyName))) {
							HColumnDescriptor family = table.getFamily(Bytes
									.toBytes(familyName));

							if (!family.isLobStoreEnabled()) {
								removeBlobStore(tableName, familyName);
							}
						} else {
							removeBlobStore(tableName, familyName);
						}
					} else {
						removeBlobStores(getBlobStores(tableName));
					}
				}
		} catch (IOException e) {
			LOG.error("Failed to clean obsolete stores in the BlobStoreManager!");
		}
	}

	public static BlobStoreManager getInstance() throws IOException {
		return manager;
	}

	public static class BlobStoreKey {
		private String tableName;
		private String familyName;

		public BlobStoreKey(String tableName, String familyName) {
			this.tableName = tableName;
			this.familyName = familyName;
		}

		public String getTableName() {
			return this.tableName;
		}

		public String getFamilyName() {
			return this.familyName;
		}

		public String toString() {
			return this.tableName + "/" + this.familyName;
		}

		public int hashCode() {
			return this.tableName.hashCode() * 127 + this.familyName.hashCode();
		}

		public boolean equals(Object o) {
			if (null == o) {
				return false;
			}
			if ((o instanceof BlobStoreKey)) {
				BlobStoreKey k = (BlobStoreKey) o;

				boolean equalTable = this.tableName == null ? false
						: k.tableName == null ? true : this.tableName
								.equals(k.tableName);

				boolean equalFamily = this.familyName == null ? false
						: k.familyName == null ? true : this.familyName
								.equals(k.familyName);

				return (equalTable) && (equalFamily);
			}

			return false;
		}
	}
}