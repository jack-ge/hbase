package org.apache.hadoop.hbase.blobStore.compactions;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.blobStore.BlobFile;
import org.apache.hadoop.hbase.blobStore.BlobFileScanner;
import org.apache.hadoop.hbase.blobStore.BlobStore;
import org.apache.hadoop.hbase.blobStore.BlobStoreManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MemStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class SweepReducer extends TableReducer<Text, KeyValue, Writable> {
	private static final SimpleDateFormat formatter = new SimpleDateFormat(
			"yyyyMMdd");

	private static final Log LOG = LogFactory.getLog(SweepReducer.class);
	private MemStoreOperator memstore;
	private Configuration conf;
	private FileSystem fs;
	private DFSClient dfsClient;
	private String tableName;
	private String family;
	private IPartition global;
	private Path familyDir;
	private Path rootDir;
	private BlobStore blobStore;
	private HColumnDescriptor columnDescriptor;
	private CacheConfig cacheConf;
	private HTable table;

	public SweepReducer() {
		this.global = null;
	}

	protected void setup(
			Reducer<Text, KeyValue, Writable, Writable>.Context context)
			throws IOException, InterruptedException {
		this.conf = context.getConfiguration();
		this.memstore = new MemStoreOperator(context, new MemStore());
		this.fs = FileSystem.get(this.conf);
		this.dfsClient = new DFSClient(this.conf);
		this.tableName = this.conf.get("hbase.mapreduce.inputtable");
		this.family = this.conf.get("hbase.mapreduce.scan.column.family");
		this.global = new GlobalPartition(context);

		Path hbaseDir = new Path(this.conf.get("hbase.rootdir"));
		this.rootDir = new Path(hbaseDir, "blobstore");
		this.familyDir = new Path(this.rootDir, this.tableName + "/"
				+ this.family);

		BlobStoreManager.getInstance().init(this.fs, this.rootDir);
		this.blobStore = BlobStoreManager.getInstance().getBlobStore(
				this.tableName, this.family);

		this.columnDescriptor = this.blobStore.getColumnDescriptor();
		this.cacheConf = new CacheConfig(this.conf, this.columnDescriptor);

		this.table = new HTable(this.conf, this.tableName);
		this.table.setAutoFlush(false);

		this.table.setWriteBufferSize(1048576L);
	}

	protected void cleanup(
			Reducer<Text, KeyValue, Writable, Writable>.Context context)
			throws IOException, InterruptedException {
		this.global.close();
	}

	public void run(Reducer<Text, KeyValue, Writable, Writable>.Context context)
			throws IOException, InterruptedException {
		setup(context);

		PartitionId id = null;
		IPartition partition = null;
		while (context.nextKey()) {
			Text key = (Text) context.getCurrentKey();
			id = PartitionId.create(key.toString());

			if ((null == partition) || (!id.equals(partition.getId()))) {
				if (null != partition) {
					partition.close();
				}
				partition = createPartition(id, context);
			}
			partition.reduce((Text) context.getCurrentKey(),
					context.getValues());
		}
		if (null != partition) {
			partition.close();
		}
		context.write(new Text("Reduce Task Done"), new Text(""));
		cleanup(context);
	}

	private IPartition createPartition(PartitionId id,
			Reducer<Text, KeyValue, Writable, Writable>.Context context)
			throws IOException {
		Partition partition = new Partition(id, context);
		return new ManagedPartition(partition, this.global);
	}

	static class PartitionId {
		private String date;
		private String startKey;

		public PartitionId(BlobFilePath filePath) {
			this.date = filePath.getDate();
			this.startKey = filePath.getStartKey();
		}

		public PartitionId(String date, String startKey) {
			this.date = date;
			this.startKey = startKey;
		}

		public static PartitionId create(String key) {
			return new PartitionId(BlobFilePath.create(key));
		}

		public boolean equals(Object anObject) {
			if (this == anObject) {
				return true;
			}
			if ((anObject instanceof PartitionId)) {
				PartitionId another = (PartitionId) anObject;
				if ((this.date.equals(another.getDate()))
						&& (this.startKey.equals(another.getStartKey()))) {
					return true;
				}
			}
			return false;
		}

		public String getDate() {
			return this.date;
		}

		public String getStartKey() {
			return this.startKey;
		}

		public void setDate(String date) {
			this.date = date;
		}

		public void setStartKey(String startKey) {
			this.startKey = startKey;
		}
	}

	static abstract interface IPartition {
		public abstract SweepReducer.PartitionId getId();

		public abstract int getSize();

		public abstract void close() throws IOException;

		public abstract void reduce(Text paramText,
				Iterable<KeyValue> paramIterable) throws IOException,
				InterruptedException;
	}

	static class FileInfo {
		private int count;
		private int referenceCount;
		private BlobFilePath blobPath;
		private long length;
		private static final float CLEANUP_THRESHOLD = 0.3F;
		private static final float COMPACT_THRESHOLD = 67108864.0F;

		public FileInfo(FileStatus status) {
			Path path = status.getPath();
			String fileName = path.getName();
			String parentName = path.getParent().getName();

			this.length = status.getLen();

			this.blobPath = BlobFilePath.create(parentName, fileName);
			this.count = this.blobPath.getRecordCount();
			this.referenceCount = 0;
		}

		public void addReference() {
			this.referenceCount += 1;
		}

		public int getReferenceCount() {
			return this.referenceCount;
		}

		public boolean needClean() {
			if (this.referenceCount >= this.count) {
				return false;
			}

			return this.count - this.referenceCount > CLEANUP_THRESHOLD
					* this.count;
		}

		public boolean needMerge() {
			return (float) this.length < COMPACT_THRESHOLD;
		}

		public BlobFilePath getPath() {
			return this.blobPath;
		}
	}

	class GlobalPartition implements SweepReducer.IPartition {
		private Map<BlobFilePath, SweepReducer.FileInfo> fileInfos = new HashMap<BlobFilePath, SweepReducer.FileInfo>();
		private Set<KeyValue> kvs = new HashSet<KeyValue>();
		private int size = 0;
		private int toBeCleaned = 0;
		private Reducer<Text, KeyValue, Writable, Writable>.Context context;

		public GlobalPartition(
				org.apache.hadoop.mapreduce.Reducer<Text, KeyValue, Writable, Writable>.Context context) {
			this.context = context;
		}

		public SweepReducer.PartitionId getId() {
			return null;
		}

		public void close() throws IOException {
			if ((this.size > 1) || (this.toBeCleaned > 0)) {
				Set<BlobFilePath> filePaths = this.fileInfos.keySet();
				Iterator<BlobFilePath> iter = filePaths.iterator();

				List<BlobFilePath> toBeDeleted = new ArrayList<BlobFilePath>();

				while (iter.hasNext()) {
					BlobFilePath path = iter.next();
					SweepReducer.FileInfo info = this.fileInfos.get(path);
					if ((info.needClean()) || (info.needMerge())) {
						BlobFile file = BlobFile
								.create(SweepReducer.this.fs,
										path.getAbsolutePath(SweepReducer.this.familyDir),
										SweepReducer.this.conf,
										SweepReducer.this.cacheConf);

						file.open();
						SweepReducer.PartitionId partitionId = new SweepReducer.PartitionId(
								path);
						BlobFileScanner scanner = file.getScanner();
						KeyValue kv = null;
						scanner.seek(KeyValue.createFirstOnRow(new byte[0]));
						while (null != (kv = scanner.next())) {
							KeyValue keyOnly = kv.createKeyOnly(false);
							if (this.kvs.contains(keyOnly)) {
								SweepReducer.this.memstore.addToMemstore(
										partitionId, kv);
								SweepReducer.this.memstore
										.flushMemStoreIfNecessary();
							}
						}
						scanner.close();
						file.close();
						toBeDeleted.add(path);
					}
				}
				SweepReducer.this.memstore.flushMemStore();

				for (int i = 0; i < toBeDeleted.size(); i++) {
					Path path = ((BlobFilePath) toBeDeleted.get(i))
							.getAbsolutePath(SweepReducer.this.familyDir);
					SweepReducer.LOG
							.info("Delete the file to be merged in global partition close"
									+ path.getName());
					this.context
							.getCounter(
									SweepReducer.ReducerCounter.FILE_TO_BE_MERGE_OR_CLEAN)
							.increment(1L);
					SweepReducer.this.fs.delete(path, false);
				}

				this.fileInfos.clear();
			}
		}

		public void reduce(Text fileName, Iterable<KeyValue> values)
				throws IOException, InterruptedException {
			BlobFilePath blobPath = BlobFilePath.create(fileName.toString());
			FileStatus status = SweepReducer.this.fs.getFileStatus(blobPath
					.getAbsolutePath(SweepReducer.this.familyDir));
			SweepReducer.FileInfo info = (SweepReducer.FileInfo) this.fileInfos
					.put(blobPath, new SweepReducer.FileInfo(status));

			Set<KeyValue> kvs = new HashSet<KeyValue>();

			Iterator<KeyValue> iter = values.iterator();
			while (iter.hasNext()) {
				KeyValue kv = (KeyValue) iter.next();
				if (null != kv) {
					kvs.add(kv);
					info.addReference();
				}
			}

			if ((info.needMerge()) || (info.needClean())) {
				if (info.needClean()) {
					this.toBeCleaned += 1;
				}
				this.kvs.addAll(kvs);
				this.size += 1;
			}
		}

		public int getSize() {
			return this.size;
		}
	}

	static class ManagedPartition implements SweepReducer.IPartition {
		private SweepReducer.IPartition partition;
		private SweepReducer.IPartition global;
		private boolean delegateToGlobal = false;

		public ManagedPartition(SweepReducer.IPartition partition,
				SweepReducer.IPartition global) {
			this.partition = partition;
			this.global = global;
			String date = partition.getId().getDate();
			String current = SweepReducer.formatter.format(new Date());
			if ((current.compareTo(date) > 0) && (partition.getSize() <= 1))
				this.delegateToGlobal = true;
		}

		public SweepReducer.PartitionId getId() {
			return this.partition.getId();
		}

		public void close() throws IOException {
			this.partition.close();
		}

		public void reduce(Text fileName, Iterable<KeyValue> values)
				throws IOException, InterruptedException {
			if (this.delegateToGlobal)
				this.global.reduce(fileName, values);
			else
				this.partition.reduce(fileName, values);
		}

		public int getSize() {
			return this.partition.getSize();
		}
	}

	class Partition implements SweepReducer.IPartition {
		private SweepReducer.PartitionId id;
		private Reducer<Text, KeyValue, Writable, Writable>.Context context;
		private int size = 0;
		private boolean memstoreUpdated = false;
		private boolean mergeSmall = false;
		private Map<BlobFilePath, SweepReducer.FileInfo> fileInfos = new HashMap<BlobFilePath, SweepReducer.FileInfo>();
		private ArrayList<BlobFilePath> toBeDeleted;

		public Partition(PartitionId id,
				Reducer<Text, KeyValue, Writable, Writable>.Context context)
				throws IOException {
			this.id = id;
			this.context = context;
			this.toBeDeleted = new ArrayList<BlobFilePath>();
			init();
		}

		public SweepReducer.PartitionId getId() {
			return this.id;
		}

		private void init() throws IOException {
			Path partitionPath = new Path(SweepReducer.this.familyDir,
					this.id.getDate());

			FileStatus[] fileStatus = listStatus(partitionPath,
					this.id.getStartKey());
			if (null == fileStatus) {
				return;
			}
			this.size = fileStatus.length;

			int smallFileCount = 0;
			for (int i = 0; i < fileStatus.length; i++) {
				SweepReducer.FileInfo info = new SweepReducer.FileInfo(
						fileStatus[i]);
				if (info.needMerge()) {
					smallFileCount++;
				}
				this.fileInfos.put(info.getPath(), info);
			}
			if (smallFileCount >= 2)
				this.mergeSmall = true;
		}

		public void close() throws IOException {
			if (null == this.id) {
				return;
			}

			Set<BlobFilePath> filePaths = this.fileInfos.keySet();
			Iterator<BlobFilePath> iter = filePaths.iterator();
			while (iter.hasNext()) {
				BlobFilePath path = iter.next();
				SweepReducer.FileInfo fileInfo = this.fileInfos.get(path);
				if (fileInfo.getReferenceCount() <= 0) {
					SweepReducer.this.fs.delete(
							path.getAbsolutePath(SweepReducer.this.familyDir),
							false);
					this.context.getCounter(
							SweepReducer.ReducerCounter.FILE_NO_REFERENCE)
							.increment(1L);
					this.context
							.getCounter(
									SweepReducer.ReducerCounter.FILE_TO_BE_MERGE_OR_CLEAN)
							.increment(1L);
				}

			}

			if (this.memstoreUpdated) {
				SweepReducer.this.memstore.flushMemStore();
			}

			for (int i = 0; i < this.toBeDeleted.size(); i++) {
				Path path = ((BlobFilePath) this.toBeDeleted.get(i))
						.getAbsolutePath(SweepReducer.this.familyDir);
				SweepReducer.LOG
						.info("[In Partition close] Delete the file to be merged in partition close "
								+ path.getName());

				this.context.getCounter(
						SweepReducer.ReducerCounter.FILE_TO_BE_MERGE_OR_CLEAN)
						.increment(1L);
				SweepReducer.this.fs.delete(path, false);
			}
			this.fileInfos.clear();
		}

		public void reduce(Text fileName, Iterable<KeyValue> values)
				throws IOException {
			if (null == values) {
				return;
			}

			BlobFilePath filePath = BlobFilePath.create(fileName.toString());
			SweepReducer.LOG.info("[In reduce] The file path's name: "
					+ fileName.toString());
			SweepReducer.FileInfo info = (SweepReducer.FileInfo) this.fileInfos
					.get(filePath);
			if (null == info) {
				SweepReducer.LOG
						.info("[In reduce] Cannot find the file on HDFS, probably this record is obsolte");
				return;
			}
			Set<KeyValue> kvs = new HashSet<KeyValue>();

			Iterator<KeyValue> iter = values.iterator();
			while (iter.hasNext()) {
				KeyValue kv = iter.next().deepCopy();
				if (null != kv) {
					kvs.add(kv);
					info.addReference();
				}
			}
			if ((info.needClean()) || ((this.mergeSmall) && (info.needMerge()))) {
				this.context.getCounter(
						SweepReducer.ReducerCounter.INPUT_FILE_COUNT)
						.increment(1L);
				BlobFile file = BlobFile.create(SweepReducer.this.fs,
						filePath.getAbsolutePath(SweepReducer.this.familyDir),
						SweepReducer.this.conf, SweepReducer.this.cacheConf);

				file.open();
				SweepReducer.PartitionId partitionId = new SweepReducer.PartitionId(
						filePath);
				BlobFileScanner scanner = file.getScanner();
				scanner.seek(KeyValue.createFirstOnRow(new byte[0]));
				KeyValue kv = null;

				while (null != (kv = scanner.next())) {
					KeyValue keyOnly = kv.createKeyOnly(false);
					if (kvs.contains(keyOnly)) {
						SweepReducer.this.memstore.addToMemstore(partitionId,
								kv);
						this.memstoreUpdated = true;
						SweepReducer.this.memstore.flushMemStoreIfNecessary();
					}
				}
				scanner.close();
				file.close();
				this.toBeDeleted.add(filePath);
			}
		}

		private FileStatus[] listStatus(Path p, String prefix)
				throws IOException {
			String src = getPathName(p);

			boolean done = false;
			List<FileStatus> stats = new ArrayList<FileStatus>();
			DirectoryListing thisListing = null;
			byte[] searchStart = Bytes.toBytes(prefix);
			do {
				thisListing = SweepReducer.this.dfsClient.listPaths(src,
						searchStart);
				if (thisListing == null) {
					if (stats.size() > 0) {
						return (FileStatus[]) stats
								.toArray(new FileStatus[stats.size()]);
					}
					return null;
				}

				HdfsFileStatus[] partialListing = thisListing
						.getPartialListing();
				for (HdfsFileStatus fileStatus : partialListing) {
					FileStatus status = makeQualified(fileStatus, p);
					if (status.getPath().getName().startsWith(prefix)) {
						stats.add(status);
					} else {
						done = true;
						break;
					}
				}
				searchStart = thisListing.getLastName();
			} while ((thisListing.hasMore()) && (!done));
			return (FileStatus[]) stats.toArray(new FileStatus[stats.size()]);
		}

		private FileStatus makeQualified(HdfsFileStatus f, Path parent) {
			return new FileStatus(f.getLen(), f.isDir(), f.getReplication(),
					f.getBlockSize(), f.getModificationTime(),
					f.getAccessTime(), f.getPermission(), f.getOwner(),
					f.getGroup(), f.getFullPath(parent).makeQualified(
							SweepReducer.this.fs));
		}

		private String getPathName(Path file) {
			String result = file.toUri().getPath();
			if (!DFSUtil.isValidName(result)) {
				throw new IllegalArgumentException("Pathname " + result
						+ " from " + file + " is not a valid DFS filename.");
			}

			return result;
		}

		public int getSize() {
			return this.size;
		}
	}

	class MemStoreOperator {
		private MemStore memstore;
		private long blockingMemStoreSize;
		private SweepReducer.PartitionId partitionId;
		private Reducer<Text, KeyValue, Writable, Writable>.Context context;

		public MemStoreOperator(
				org.apache.hadoop.mapreduce.Reducer<Text, KeyValue, Writable, Writable>.Context context,
				MemStore memstore) {
			this.memstore = memstore;
			this.context = context;

			long flushSize = SweepReducer.this.conf.getLong(
					"hbase.hregion.memstore.flush.size", 134217728L);

			this.blockingMemStoreSize = (flushSize * SweepReducer.this.conf
					.getLong("hbase.hregion.memstore.block.multiplier", 2L));
		}

		public void flushMemStoreIfNecessary() throws IOException {
			if (this.memstore.heapSize() >= this.blockingMemStoreSize)
				flushMemStore();
		}

		public void flushMemStore() throws IOException {
			this.memstore.snapshot();
			SortedSet<KeyValue> snapshot = this.memstore.getSnapshot();

			internalFlushCache(snapshot, 9223372036854775807L);
			this.memstore.clearSnapshot(snapshot);
		}

		private void internalFlushCache(SortedSet<KeyValue> set,
				long logCacheFlushId) throws IOException {
			Path blobFilePath = null;
			if (set.size() == 0) {
				return;
			}

			StoreFile.Writer blobWriter = null;

			blobWriter = SweepReducer.this.blobStore
					.createWriterInTmp(set.size(),
							SweepReducer.this.columnDescriptor
									.getCompactionCompression(),
							this.partitionId.getStartKey());

			blobFilePath = blobWriter.getPath();

			String targetPathName = this.partitionId.getDate();
			Path targetPath = new Path(
					SweepReducer.this.blobStore.getHomePath(), targetPathName);

			String relativePath = targetPathName + "/" + blobFilePath.getName();

			SweepReducer.LOG.info("create tmp file under "
					+ blobFilePath.toString());

			byte[] referenceValue = Bytes.add(new byte[] { 0 },
					Bytes.toBytes(relativePath));

			int keyValueCount = 0;

			KeyValueScanner scanner = new CollectionBackedScanner(set,
					KeyValue.COMPARATOR);

			scanner.seek(KeyValue.createFirstOnRow(new byte[0]));
			KeyValue kv = null;
			while (null != (kv = scanner.next())) {
				kv.setMemstoreTS(0L);
				blobWriter.append(kv);
				keyValueCount++;
			}
			scanner.close();

			blobWriter.appendMetadata(logCacheFlushId, false);
			blobWriter.appendFileInfo(StoreFile.KEYVALUE_COUNT,
					Bytes.toBytes(keyValueCount));

			blobWriter.close();

			SweepReducer.this.blobStore.commitFile(blobFilePath, targetPath);
			this.context.getCounter(
					SweepReducer.ReducerCounter.FILE_AFTER_MERGE_CLEAN)
					.increment(1L);

			scanner = new CollectionBackedScanner(set, KeyValue.COMPARATOR);
			scanner.seek(KeyValue.createFirstOnRow(new byte[0]));
			kv = null;
			while (null != (kv = scanner.next())) {
				kv.setMemstoreTS(0L);

				KeyValue reference = new KeyValue(kv.getBuffer(),
						kv.getRowOffset(), kv.getRowLength(), kv.getBuffer(),
						kv.getFamilyOffset(), kv.getFamilyLength(),
						kv.getBuffer(), kv.getQualifierOffset(),
						kv.getQualifierLength(), kv.getTimestamp(),
						KeyValue.Type.Reference, referenceValue, 0,
						referenceValue.length);

				Put put = new Put(reference.getRow());
				put.add(reference);
				SweepReducer.this.table.put(put);
				this.context.getCounter(
						SweepReducer.ReducerCounter.RECORDS_UPDATED).increment(
						1L);
			}

			if (keyValueCount > 0) {
				SweepReducer.this.table.flushCommits();
			}
			scanner.close();
		}

		public void addToMemstore(SweepReducer.PartitionId id, KeyValue kv) {
			if (null == this.partitionId) {
				this.partitionId = id;
			} else {
				String effectiveDate = this.partitionId.getDate().compareTo(
						id.getDate()) >= 0 ? this.partitionId.getDate() : id
						.getDate();

				String effectiveStartKey = this.partitionId.getStartKey()
						.compareTo(id.getStartKey()) <= 0 ? this.partitionId
						.getStartKey() : id.getStartKey();

				this.partitionId.setDate(effectiveDate);
				this.partitionId.setStartKey(effectiveStartKey);
			}
			this.memstore.add(kv);
		}
	}

	public static enum ReducerCounter {
		INPUT_FILE_COUNT, FILE_TO_BE_MERGE_OR_CLEAN, FILE_NO_REFERENCE, FILE_AFTER_MERGE_CLEAN, RECORDS_UPDATED, DISK_SPACE_RECLAIMED;
	}
}