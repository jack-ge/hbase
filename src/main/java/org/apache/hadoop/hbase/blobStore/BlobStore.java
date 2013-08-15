package org.apache.hadoop.hbase.blobStore;

import java.io.IOException;
import java.util.UUID;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.io.VersionMismatchException;

public class BlobStore {
	static final Log LOG = LogFactory.getLog(BlobStore.class);
	private FileSystem fs;
	private Path homePath;
	private CacheConfig cacheConf;
	private HColumnDescriptor family;
	private static final String TMP = ".tmp";
	private Configuration conf;
	private static final Configuration DUMBCONF = new Configuration();
	private static final int MIN_BLOCK_SIZE = 1024;

	private BlobStore(FileSystem fs, Path homedPath, CacheConfig cacheConf,
			HColumnDescriptor family) {
		this.fs = fs;
		this.homePath = homedPath;
		this.conf = new Configuration();
		this.cacheConf = cacheConf;
		this.family = family;
	}

	public static BlobStore create(FileSystem fs, Path homePath,
			HColumnDescriptor family) throws IOException {
		CacheConfig cacheConf = new CacheConfig(DUMBCONF, family);
		return new BlobStore(fs, homePath, cacheConf, family);
	}

	public static BlobStore load(FileSystem fs, Path homeDir, String tableName,
			String familyName) throws IOException {
		HColumnDescriptor family = loadFamily(fs, homeDir, tableName,
				familyName);
		if (null == family) {
			LOG.warn("failed to load the blob store, can not find family ["
					+ familyName + "] under table [" + tableName + "]!");

			return null;
		}

		if (!family.isLobStoreEnabled()) {
			LOG.warn("failed to load the blob store, the family [" + familyName
					+ "] under table [" + tableName + "] does not enable lob!");

			return null;
		}
		CacheConfig cacheConf = new CacheConfig(DUMBCONF, family);
		Path homePath = new Path(homeDir, tableName + "/" + familyName);
		return new BlobStore(fs, homePath, cacheConf, family);
	}

	private static HColumnDescriptor loadFamily(FileSystem fs, Path homeDir,
			String tableName, String familyName) throws IOException {
		TableDescriptors tableDescriptors = new FSTableDescriptors(fs,
				homeDir.getParent());

		HTableDescriptor tableDescriptor = tableDescriptors.get(tableName);
		if (tableDescriptor != null) {
			return tableDescriptor.getFamily(Bytes.toBytes(familyName));
		}
		return null;
	}

	public HColumnDescriptor getColumnDescriptor() {
		return this.family;
	}

	private static Path getTmpDir(Path home) {
		return new Path(home, TMP);
	}

	public Path getHomePath() {
		return this.homePath;
	}

	public String getTableName() {
		return this.homePath.getParent().getName();
	}

	public String getFamilyName() {
		return this.homePath.getName();
	}

	public StoreFile.Writer createWriterInTmp(int maxKeyCount,
			Compression.Algorithm compression, HRegionInfo regionStartKey)
			throws IOException {
		byte[] startKey = regionStartKey.getStartKey();
		if ((null == startKey) || (startKey.length == 0)) {
			startKey = new byte[1];
			startKey[0] = 0;
		}

		CRC32 crc = new CRC32();
		crc.update(startKey);
		int checksum = (int) crc.getValue();
		return createWriterInTmp(maxKeyCount, compression,
				BlobFilePath.int2HexString(checksum));
	}

	public StoreFile.Writer createWriterInTmp(int maxKeyCount,
			Compression.Algorithm compression, String prefix)
			throws IOException {
		Path path = getTmpDir();

		CacheConfig writerCacheConf = this.cacheConf;

		BlobFilePath blobPath = BlobFilePath.create(prefix, maxKeyCount, null,
				UUID.randomUUID().toString().replaceAll("-", ""));

		Path file = new Path(path, blobPath.getFileName());

		StoreFile.Writer w = new StoreFile.WriterBuilder(this.conf,
				writerCacheConf, this.fs, MIN_BLOCK_SIZE).withFilePath(file)
				.withDataBlockEncoder(NoOpDataBlockEncoder.INSTANCE)
				.withComparator(KeyValue.COMPARATOR)
				.withBloomType(StoreFile.BloomType.NONE)
				.withMaxKeyCount(maxKeyCount)
				.withChecksumType(HFile.DEFAULT_CHECKSUM_TYPE)
				.withBytesPerChecksum(16384).withCompression(compression)
				.withReplication(this.family.getReplication()).build();

		return w;
	}

	public KeyValue resolve(KeyValue reference) throws IOException {
		KeyValue search = reference.clone();
		search.setType(KeyValue.Type.Put);
		byte[] referenceValue = search.getValue();
		if (referenceValue.length < 1) {
			return null;
		}
		byte blobStoreVersion = referenceValue[0];
		if (blobStoreVersion > 0) {
			throw new VersionMismatchException((byte) 0, blobStoreVersion);
		}

		String fileName = Bytes.toString(referenceValue, 1,
				referenceValue.length - 1);

		KeyValue result = null;

		Path targetPath = new Path(this.homePath, fileName);
		BlobFile file = this.cacheConf.getBlobFileCache().open(this.fs,
				targetPath, this.cacheConf);

		if (null != file) {
			result = file.readKeyValue(search);
			file.close();
		}

		return result;
	}

	public void commitFile(Path sourceFile, Path targetPath) throws IOException {
		if (null == sourceFile) {
			throw new NullPointerException();
		}

		Path dstPath = new Path(targetPath, sourceFile.getName());
		validateStoreFile(sourceFile);
		String msg = "Renaming flushed file at " + sourceFile + " to "
				+ dstPath;
		LOG.info(msg);

		Path parent = dstPath.getParent();
		if (!this.fs.exists(parent)) {
			this.fs.mkdirs(parent);
		}

		if (!this.fs.rename(sourceFile, dstPath))
			LOG.warn("Unable to rename " + sourceFile + " to " + dstPath);
	}

	private void validateStoreFile(Path path) throws IOException {
		StoreFile storeFile = null;
		try {
			storeFile = new StoreFile(this.fs, path, this.conf, this.cacheConf,
					StoreFile.BloomType.NONE, NoOpDataBlockEncoder.INSTANCE);

			storeFile.createReader();
		} catch (IOException e) {
			LOG.error("Failed to open store file : " + path
					+ ", keeping it in tmp location", e);

			throw e;
		} finally {
			if (storeFile != null)
				storeFile.closeReader(false);
		}
	}

	public Path getTmpDir() {
		return getTmpDir(this.homePath);
	}
}