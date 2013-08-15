package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OrdinalIncrementalSplitPolicy extends RegionSplitPolicy {
	private static final Log LOG = LogFactory
			.getLog(OrdinalIncrementalSplitPolicy.class);

	private byte[] splitKey = null;
	private int ordinalLength = 0;
	public static final String ORDINAL_LENGTH = "ordinal_incremental_split_policy.ordinal_length";

	protected void configureForRegion(HRegion region) {
		super.configureForRegion(region);
		if (region != null) {
			this.ordinalLength = 0;

			String ordinalLengthString = region.getTableDesc().getValue(
					ORDINAL_LENGTH);

			if (ordinalLengthString == null) {
				LOG.error("ordinal_incremental_split_policy.ordinal_length not specified for table "
						+ region.getTableDesc().getNameAsString());

				return;
			}
			try {
				this.ordinalLength = Integer.parseInt(ordinalLengthString);
			} catch (NumberFormatException nfe) {
			}
			if (this.ordinalLength <= 0)
				LOG.error("Invalid value for ordinal_incremental_split_policy.ordinal_length for table "
						+ region.getTableDesc().getNameAsString()
						+ ":"
						+ ordinalLengthString);
		}
	}

	protected boolean shouldSplit() {
		boolean shouldSplit = false;

		if (this.ordinalLength <= 0) {
			return false;
		}

		byte[] endKey = this.region.getEndKey();
		if ((null == endKey) || (endKey.length == 0)) {
			shouldSplit = true;
			byte[] startKey = this.region.getStartKey();
			this.splitKey = new byte[this.ordinalLength];
			System.arraycopy(startKey, 0, this.splitKey, 0,
					Math.min(startKey.length, this.ordinalLength));
			increment(this.splitKey);
		} else {
			this.splitKey = null;
		}

		return shouldSplit;
	}

	protected byte[] getSplitPoint() {
		return this.splitKey;
	}

	private final void increment(byte[] input) {
		int length = input.length;
		boolean carry = true;

		for (int i = length - 1; (i >= 0) && (carry); i--) {
			input[i] = (byte) (input[i] + 1 & 0xFF);
			carry = 0 == input[i];
		}
	}
}