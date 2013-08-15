package org.apache.hadoop.hbase.util;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ThreadSafeSimpleDateFormat extends DateFormat {
	private static final long serialVersionUID = 1L;
	private DateFormat df;

	public ThreadSafeSimpleDateFormat(String format) {
		this.df = new SimpleDateFormat(format);
	}

	public synchronized StringBuffer format(Date date, StringBuffer toAppendTo,
			FieldPosition fieldPosition) {
		return this.df.format(date, toAppendTo, fieldPosition);
	}

	public synchronized Date parse(String source, ParsePosition pos) {
		return this.df.parse(source, pos);
	}
}