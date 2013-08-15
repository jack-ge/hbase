package org.apache.hadoop.hbase.blobStore.compactions;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.blobStore.BlobStore;
import org.apache.hadoop.hbase.blobStore.BlobStoreUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ReferenceOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SweepJob {
	private FileSystem fs;
	static final Logger logger = LoggerFactory.getLogger(SweepJob.class);

	public SweepJob(FileSystem fs) {
		this.fs = fs;
	}

	public void sweep(BlobStore store, Configuration conf) throws IOException,
			ClassNotFoundException, InterruptedException {
		BlobStoreUtils.cleanObsoleteData(this.fs, store);

		Scan scan = new Scan();
		scan.setResolveReference(false);
		scan.setFilter(new ReferenceOnlyFilter());
		scan.setCaching(10000);

		String table = store.getTableName();
		String family = store.getFamilyName();

		Job job = prepareTableJob(table, scan, SweepMapper.class, Text.class,
				KeyValue.class, SweepReducer.class, Text.class, Writable.class,
				TextOutputFormat.class, conf);

		job.getConfiguration()
				.set("hbase.mapreduce.scan.column.family", family);
		job.setPartitionerClass(SweepPartitioner.class);
		job.waitForCompletion(true);
	}

	protected Job prepareTableJob(String table, Scan scan,
			Class<? extends TableMapper> mapper,
			Class<? extends Writable> mapOutputKey,
			Class<? extends Writable> mapOutputValue,
			Class<? extends Reducer> reducer,
			Class<? extends WritableComparable> reduceOutputKey,
			Class<? extends Writable> reduceOutputValue,
			Class<? extends OutputFormat> outputFormat, Configuration conf)
			throws IOException {
		Job job = new Job(conf);

		if (reducer.equals(Reducer.class)) {
			if (mapper.equals(Mapper.class)) {
				logger.error(new IllegalStateException(
						"Can't figure out the user class jar file from mapper/reducer")
						.getMessage());

				throw new IllegalStateException(
						"Can't figure out the user class jar file from mapper/reducer");
			}

			job.setJarByClass(mapper);
		} else {
			job.setJarByClass(reducer);
		}

		TableMapReduceUtil.initTableMapperJob(table, scan, mapper,
				reduceOutputKey, reduceOutputValue, job);

		job.setInputFormatClass(TableInputFormat.class);
		job.setMapOutputKeyClass(mapOutputKey);
		job.setMapOutputValueClass(mapOutputValue);
		job.setReducerClass(reducer);
		job.setOutputFormatClass(outputFormat);
		String jobName = getCustomJobName(getClass().getSimpleName(), mapper,
				reducer, table);

		Path tmpDir = new Path(conf.get("mapred.temp.dir",
				"/tmp/hadoop/mapred/tmp"));
		Path outputPath = new Path(tmpDir, new StringBuilder().append(jobName)
				.append("_output").append("/")
				.append(UUID.randomUUID().toString()).toString());

		FileOutputFormat.setOutputPath(job, outputPath);
		job.setJobName(jobName);
		return job;
	}

	private static String getCustomJobName(String className,
			Class<? extends Mapper> mapper, Class<? extends Reducer> reducer,
			String notion) {
		StringBuilder name = new StringBuilder(200);
		name.append(className);
		name.append('-').append(mapper.getSimpleName());
		name.append('-').append(reducer.getSimpleName());
		if (null != notion) {
			name.append('-').append(notion);
		}
		return name.toString();
	}
}