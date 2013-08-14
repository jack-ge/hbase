package org.apache.hadoop.hbase.filter;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.ByteBloomFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class InComparator extends WritableByteArrayComparable {
	private ByteBloomFilter bloomFilter;
	private ByteBuffer bloom;
	private Set<byte[]> valueSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

	public InComparator() {
	}

	public InComparator(byte[][] values) {
		this.bloomFilter = new ByteBloomFilter(values.length,
				0.009999999776482582D, 1, 0);
		this.bloomFilter.allocBloom();

		for (byte[] value : values) {
			this.valueSet.add(value);
			this.bloomFilter.add(value);
		}

		initBloom();
	}

	public InComparator(String fileName) throws IOException {
		File file = new File(fileName);
		if ((!file.exists()) || (!file.isFile())) {
			throw new IOException("File does not exist.");
		}
		Scanner scanner = new Scanner(file);
		List<byte[]> values = new ArrayList();
		while (scanner.hasNext()) {
			values.add(Bytes.toBytes(scanner.nextLine()));
		}

		this.bloomFilter = new ByteBloomFilter(values.size(),
				0.009999999776482582D, 1, 0);
		this.bloomFilter.allocBloom();

		for (byte[] value : values) {
			this.valueSet.add(value);
			this.bloomFilter.add(value);
		}

		initBloom();
	}

	public void initBloom() {
		ByteArrayOutputStream bOut = new ByteArrayOutputStream();
		try {
			this.bloomFilter.writeBloom(new DataOutputStream(bOut));
		} catch (IOException e) {
		}
		this.bloom = ByteBuffer.wrap(bOut.toByteArray());
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.valueSet.size());
		for (byte[] value : this.valueSet)
			Bytes.writeByteArray(out, value);
	}

	public void readFields(DataInput in) throws IOException {
		int numValues = in.readInt();
		this.bloomFilter = new ByteBloomFilter(numValues,
				0.009999999776482582D, 1, 0);
		this.bloomFilter.allocBloom();
		for (int i = 0; i < numValues; i++) {
			byte[] value = Bytes.readByteArray(in);
			this.valueSet.add(value);
			this.bloomFilter.add(value);
		}

		initBloom();
	}

	public int compareTo(byte[] value, int offset, int length) {
		if ((this.bloomFilter.contains(value, offset, length, this.bloom))
				&& (this.valueSet.contains(value))) {
			return 0;
		}

		return -1;
	}
}