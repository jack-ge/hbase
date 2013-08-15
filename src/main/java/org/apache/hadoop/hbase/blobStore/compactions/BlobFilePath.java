package org.apache.hadoop.hbase.blobStore.compactions;

import java.security.InvalidParameterException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ThreadSafeSimpleDateFormat;

public class BlobFilePath {
	private String date;
	private int startKey;
	private String uuid;
	private int count;
	private static final ThreadSafeSimpleDateFormat dateFormatter = new ThreadSafeSimpleDateFormat(
			"yyyyMMdd");

	static final char[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8',
			'9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
			'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y',
			'z' };

	public static BlobFilePath create(String startKey, int count, Date date,
			String uuid) {
		String dateString = null;
		if (null != date) {
			dateString = dateFormatter.format(date);
		}
		return new BlobFilePath(dateString, startKey, count, uuid);
	}

	public static BlobFilePath create(String filePath) {
		int slashPosition = filePath.indexOf("/");
		String parent = null;
		String fileName = null;
		if (-1 != slashPosition) {
			parent = filePath.substring(0, slashPosition);
			fileName = filePath.substring(slashPosition + 1);
		} else {
			fileName = filePath;
		}
		return create(parent, fileName);
	}

	public static BlobFilePath create(String parentName, String fileName) {
		String date = parentName;
		int startKey = hexString2Int(fileName.substring(0, 8));
		int count = hexString2Int(fileName.substring(8, 16));
		String uuid = fileName.substring(16);
		return new BlobFilePath(date, startKey, count, uuid);
	}

	public static String int2HexString(int i) {
		int shift = 4;
		char[] buf = new char[8];

		int charPos = 8;
		int radix = 1 << shift;
		int mask = radix - 1;
		do {
			charPos--;
			buf[charPos] = digits[(i & mask)];
			i >>>= shift;
		} while (charPos > 0);

		return new String(buf);
	}

	public static int hexString2Int(String hex) {
		byte[] buffer = Bytes.toBytes(hex);
		if (buffer.length != 8) {
			throw new InvalidParameterException(
					"hexString2Int length not valid");
		}

		for (int i = 0; i < buffer.length; i++) {
			byte ch = buffer[i];
			if ((ch >= 97) && (ch <= 122)) {
				buffer[i] = (byte) (ch - 97 + 10);
			} else {
				buffer[i] = (byte) (ch - 48);
			}
		}

		buffer[0] = (byte) (buffer[0] << 4 ^ buffer[1]);
		buffer[1] = (byte) (buffer[2] << 4 ^ buffer[3]);
		buffer[2] = (byte) (buffer[4] << 4 ^ buffer[5]);
		buffer[3] = (byte) (buffer[6] << 4 ^ buffer[7]);
		return Bytes.toInt(buffer, 0, 4);
	}

	public BlobFilePath(String date, String startKey, int count, String uuid) {
		this(date, hexString2Int(startKey), count, uuid);
	}

	public BlobFilePath(String date, int startKey, int count, String uuid) {
		this.startKey = startKey;
		this.count = count;
		this.uuid = uuid;
		this.date = date;
	}

	public String getStartKey() {
		return int2HexString(this.startKey);
	}

	public String getDate() {
		return this.date;
	}

	public int hashCode() {
		StringBuilder builder = new StringBuilder();
		builder.append(this.date);
		builder.append(this.startKey);
		builder.append(this.uuid);
		builder.append(this.count);
		return builder.toString().hashCode();
	}

	public boolean equals(Object anObject) {
		if (this == anObject) {
			return true;
		}
		if ((anObject instanceof BlobFilePath)) {
			BlobFilePath another = (BlobFilePath) anObject;
			if ((this.date.equals(another.date))
					&& (this.startKey == another.startKey)
					&& (this.uuid.equals(another.uuid))
					&& (this.count == another.count)) {
				return true;
			}
		}
		return false;
	}

	public int getRecordCount() {
		return this.count;
	}

	public Path getAbsolutePath(Path rootPath) {
		if (null == this.date) {
			return new Path(rootPath, getFileName());
		}

		return new Path(rootPath, this.date + "/" + getFileName());
	}

	public String getFileName() {
		return int2HexString(this.startKey) + int2HexString(this.count)
				+ this.uuid;
	}
}