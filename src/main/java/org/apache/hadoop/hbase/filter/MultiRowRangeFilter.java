package org.apache.hadoop.hbase.filter;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.WeakHashMap;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class MultiRowRangeFilter extends FilterBase {
	private static final Log LOG = LogFactory.getLog(MultiRowRangeFilter.class);
	private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
	private static final char DEFAULT_DELIMITER = ' ';
	private static WeakHashMap<String, RowKeyRangeList> rangesMap = new WeakHashMap();
	private SourceType type;
	private Path path;
	private List<RowKeyRange> rangeList;
	private String rangeKey;
	private char delimiter = ' ';
	private Pattern pattern = WHITESPACE_PATTERN;

	private boolean done = false;
	private boolean initialized = false;
	private int index;
	private RowKeyRange range;
	private Filter.ReturnCode currentReturnCode;

	public MultiRowRangeFilter() {
	}

	public MultiRowRangeFilter(List<RowKeyRange> list) throws IOException {
		this.type = SourceType.LIST;
		this.rangeList = list;
		check(this.rangeList, true);
	}

	public MultiRowRangeFilter(Path path) throws IOException {
		this(path, ' ', WHITESPACE_PATTERN);
	}

	public MultiRowRangeFilter(Path path, char delimiter) throws IOException {
		this(path, delimiter, WHITESPACE_PATTERN);
	}

	public MultiRowRangeFilter(Path path, Pattern pattern) throws IOException {
		this(path, ' ', pattern);
	}

	private MultiRowRangeFilter(Path path, char delimiter, Pattern pattern)
			throws IOException {
		this.type = SourceType.FILE;
		this.path = path;
		this.delimiter = delimiter;
		this.pattern = pattern;
		initRangesFromFile();
		check(this.rangeList, true);
	}

	public boolean filterAllRemaining() {
		return this.done;
	}

	public boolean filterRowKey(byte[] buffer, int offset, int length) {
		if (this.rangeList.size() == 0) {
			this.done = true;
			this.currentReturnCode = Filter.ReturnCode.NEXT_ROW;
			return false;
		}

		if ((!this.initialized)
				|| (!this.range.contains(buffer, offset, length))) {
			byte[] rowkey = new byte[length];
			System.arraycopy(buffer, offset, rowkey, 0, length);
			this.index = getNextRangeIndex(rowkey);
			if (this.index >= this.rangeList.size()) {
				this.done = true;
				this.currentReturnCode = Filter.ReturnCode.NEXT_ROW;
				return false;
			}
			this.range = ((RowKeyRange) this.rangeList.get(this.index));
			this.initialized = true;
		}

		if (Bytes.compareTo(buffer, offset, length, this.range.startRow, 0,
				this.range.startRow.length) < 0) {
			this.currentReturnCode = Filter.ReturnCode.SEEK_NEXT_USING_HINT;
		} else
			this.currentReturnCode = Filter.ReturnCode.INCLUDE;

		return false;
	}

	public Filter.ReturnCode filterKeyValue(KeyValue kv) {
		return this.currentReturnCode;
	}

	public KeyValue getNextKeyHint(KeyValue currentKV) {
		return KeyValue.createFirstOnRow(this.range.startRow);
	}

	public void readFields(DataInput in) throws IOException {
		this.type = SourceType.valueOf(in.readUTF());
		switch (this.type.ordinal()) {
		case 1:
			int length = in.readInt();
			this.rangeList = new ArrayList(length);
			for (int i = 0; i < length; i++) {
				RowKeyRange range = new RowKeyRange();
				range.readFields(in);
				this.rangeList.add(range);
			}
			break;
		case 2:
			this.path = new Path(in.readUTF());
			this.delimiter = in.readChar();
			this.pattern = Pattern.compile(in.readUTF());
			initRangesFromFile();
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.type.name());
		switch (this.type.ordinal()) {
		case 1:
			out.writeInt(this.rangeList.size());
			for (RowKeyRange range : this.rangeList) {
				range.write(out);
			}
			break;
		case 2:
			out.writeUTF(this.path.toString());
			out.writeChar(this.delimiter);
			out.writeUTF(this.pattern.toString());
		}
	}

	private int getNextRangeIndex(byte[] rowKey) {
		RowKeyRange temp = new RowKeyRange(rowKey, null);
		int index = Collections.binarySearch(this.rangeList, temp);
		if (index < 0) {
			int insertionPosition = -index - 1;

			if ((insertionPosition != 0)
					&& (((RowKeyRange) this.rangeList
							.get(insertionPosition - 1)).contains(rowKey))) {
				return insertionPosition - 1;
			}
			return insertionPosition;
		}

		return index;
	}

	private void initRangesFromFile() throws IOException {
		FileSystem fs = this.path.getFileSystem(HBaseConfiguration.create());
		FileStatus status = fs.getFileStatus(this.path);
		long modTime = status.getModificationTime();
		long size = status.getLen();
		this.rangeKey = new StringBuilder().append(this.path.toString())
				.append(size).append(modTime).append(this.delimiter)
				.append(this.pattern.toString()).toString();
		if (rangesMap.get(this.rangeKey) == null) {
			synchronized (rangesMap) {
				if (rangesMap.get(this.rangeKey) == null) {
					rangesMap.put(this.rangeKey, new RowKeyRangeList(fs,
							this.path));
				}
			}
		}
		this.rangeList = ((RowKeyRangeList) rangesMap.get(this.rangeKey))
				.getRowKeyRangeList();
	}

	private void check(List<RowKeyRange> ranges, boolean details)
			throws IOException {
		if (ranges.size() == 0) {
			throw new IOException("No ranges found.");
		}
		List<RowKeyRange> invalidRanges = new ArrayList();
		List<Integer> overlaps = new ArrayList();

		if (!((RowKeyRange) ranges.get(0)).isValid())
			invalidRanges.add(ranges.get(0));
		byte[] lastStopRow = ((RowKeyRange) ranges.get(0)).stopRow;

		for (int i = 1; i < ranges.size(); i++) {
			RowKeyRange range = (RowKeyRange) ranges.get(i);
			if (!range.isValid()) {
				invalidRanges.add(range);
			}
			if ((Bytes.equals(lastStopRow, HConstants.EMPTY_BYTE_ARRAY))
					|| (Bytes.compareTo(lastStopRow, range.startRow) > 0)) {
				overlaps.add(Integer.valueOf(i));
			}
			lastStopRow = range.stopRow;
		}

		if ((invalidRanges.size() != 0) || (overlaps.size() != 0)) {
			StringBuilder sb = new StringBuilder();
			sb.append(invalidRanges.size()).append(" invaild ranges.\n");
			if (details) {
				for (RowKeyRange range : invalidRanges) {
					sb.append(
							new StringBuilder()
									.append("Invalid range: start row=>")
									.append(Bytes.toString(range.startRow))
									.append(", stop row => ")
									.append(Bytes.toString(range.startRow))
									.toString()).append('\n');
				}

			}

			sb.append("There might be overlaps between rowkey ranges or the rowkey ranges are not arranged in ascending order.\n");
			sb.append(overlaps.size()).append(
					" overlap or unsorted row key range pairs.\n");
			if (details) {
				for (Integer over : overlaps) {
					sb.append(
							new StringBuilder()
									.append("Overlap or unsorted range pair: start row=>")
									.append(Bytes.toString(((RowKeyRange) ranges
											.get(over.intValue() - 1)).startRow))
									.append(", stop row => ")
									.append(Bytes.toString(((RowKeyRange) ranges
											.get(over.intValue() - 1)).stopRow))
									.toString()).append('\n');

					sb.append(
							new StringBuilder()
									.append("                           and  start row=>")
									.append(Bytes.toString(((RowKeyRange) ranges
											.get(over.intValue())).startRow))
									.append(", stop row => ")
									.append(Bytes.toString(((RowKeyRange) ranges
											.get(over.intValue())).stopRow))
									.toString()).append('\n');
				}

			}

			throw new IOException(sb.toString());
		}
	}

	public Path getPath() {
		return this.path;
	}

	public char getDelimiter() {
		return this.delimiter;
	}

	public Pattern getPattern() {
		return this.pattern;
	}

	public class RowKeyRangeList {
		private FileSystem fs;
		private Path path;
		private List<MultiRowRangeFilter.RowKeyRange> rowKeyRangeList;

		public RowKeyRangeList(FileSystem fs, Path path) {
			this.fs = fs;
			this.path = path;
		}

		public synchronized List<MultiRowRangeFilter.RowKeyRange> getRowKeyRangeList()
				throws IOException {
			if (this.rowKeyRangeList == null) {
				if (MultiRowRangeFilter.LOG.isDebugEnabled()) {
					MultiRowRangeFilter.LOG
							.debug("Initialize the row key ranges from file: "
									+ this.path.toString());
				}

				this.rowKeyRangeList = new ArrayList();
				FSDataInputStream input = null;
				BufferedReader d = null;
				try {
					input = this.fs.open(this.path);
					d = new BufferedReader(new InputStreamReader(input));
					String line;
					while ((line = d.readLine()) != null) {
						if (line.trim().isEmpty())
							continue;
						String[] rowKeys = splitRangeLine(line,
								MultiRowRangeFilter.this.delimiter);
						if (rowKeys.length < 2) {
							throw new IOException("Line: " + line
									+ " cannot be splited.");
						}
						this.rowKeyRangeList
								.add(new MultiRowRangeFilter.RowKeyRange(
										rowKeys[0], rowKeys[1]));
					}
				} finally {
					if (d != null) {
						d.close();
					}
					if (input != null) {
						input.close();
					}
				}
			}
			return this.rowKeyRangeList;
		}

		private String[] splitRangeLine(String line, char delim) {
			if (delim == ' ') {
				return MultiRowRangeFilter.this.pattern.split(line, 2);
			}
			int index = line.indexOf(delim);
			return new String[] {
					line.substring(0, index),
					line.indexOf(delim) == -1 ? line : line
							.substring(index + 1) };
		}
	}

	public static class RowKeyRange implements Writable,
			Comparable<RowKeyRange> {
		private byte[] startRow;
		private byte[] stopRow;
		private int isScan = 0;

		public RowKeyRange() {
		}

		public RowKeyRange(String startRow, String stopRow) {
			this(
					(startRow == null) || (startRow.isEmpty()) ? HConstants.EMPTY_BYTE_ARRAY
							: Bytes.toBytes(startRow),
					(stopRow == null) || (stopRow.isEmpty()) ? HConstants.EMPTY_BYTE_ARRAY
							: Bytes.toBytes(stopRow));
		}

		public RowKeyRange(byte[] startRow, byte[] stopRow) {
			this.startRow = startRow;
			this.stopRow = stopRow;
			this.isScan = (Bytes.equals(startRow, stopRow) ? 1 : 0);
		}

		public void readFields(DataInput in) throws IOException {
			this.startRow = Bytes.readByteArray(in);
			this.stopRow = Bytes.readByteArray(in);
			this.isScan = (Bytes.equals(this.startRow, this.stopRow) ? 1 : 0);
		}

		public void write(DataOutput out) throws IOException {
			Bytes.writeByteArray(out, this.startRow);
			Bytes.writeByteArray(out, this.stopRow);
		}

		public boolean contains(byte[] row) {
			return contains(row, 0, row.length);
		}

		public boolean contains(byte[] buffer, int offset, int length) {
			return (Bytes.compareTo(buffer, offset, length, this.startRow, 0,
					this.startRow.length) >= 0)
					&& ((Bytes
							.equals(this.stopRow, HConstants.EMPTY_BYTE_ARRAY)) || (Bytes
							.compareTo(buffer, offset, length, this.stopRow, 0,
									this.stopRow.length) < this.isScan));
		}

		public int compareTo(RowKeyRange other) {
			return Bytes.compareTo(this.startRow, other.startRow);
		}

		public boolean isValid() {
			return (Bytes.equals(this.stopRow, HConstants.EMPTY_BYTE_ARRAY))
					|| (Bytes.compareTo(this.startRow, this.stopRow) <= 0);
		}
	}

	public static enum SourceType {
		FILE, LIST;
	}
}