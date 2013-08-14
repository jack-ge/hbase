package org.apache.hadoop.hbase.coprocessor.batch;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class BatchExecClient {
	private static final Log LOG = LogFactory.getLog(BatchExecClient.class);

	public static <T extends CoprocessorProtocol, R> void coprocessorExec(
			HTable table, Class<T> protocol, BatchExecCall<T, R> call,
			BatchExecCall.ServerCallback<R> serverCallback,
			BatchExecCall.ClientCallback<R> callback) throws IOException,
			Throwable {
		List<Pair<Pair<byte[], byte[]>, BatchExecCall<T, R>>> callsByKeyRange = new ArrayList<Pair<Pair<byte[], byte[]>, BatchExecCall<T, R>>>();

		callsByKeyRange
				.add(new Pair<Pair<byte[], byte[]>, BatchExecCall<T, R>>(
						new Pair<byte[], byte[]>(HConstants.EMPTY_START_ROW,
								HConstants.EMPTY_END_ROW), call));

		coprocessorExec(table, protocol, callsByKeyRange, serverCallback,
				callback);
	}

	public static <T extends CoprocessorProtocol, R> void coprocessorExec(
			HTable table,
			Class<T> protocol,
			List<Pair<Pair<byte[], byte[]>, BatchExecCall<T, R>>> callsByKeyRange,
			BatchExecCall.ServerCallback<R> serverCallback,
			BatchExecCall.ClientCallback<R> callback) throws IOException,
			Throwable {
		Map<ServerName, List<Pair<byte[], BatchExecCall<T, R>>>> callsByServer = new HashMap<ServerName, List<Pair<byte[], BatchExecCall<T, R>>>>();

		BatchExecCall<T, R> call;
		byte[] start;
		byte[] end;

		NavigableMap<HRegionInfo, ServerName> regions = table
				.getRegionLocations();
		for (Pair<Pair<byte[], byte[]>, BatchExecCall<T, R>> callPair : callsByKeyRange) {
			call = (BatchExecCall<T, R>) callPair.getSecond();
			start = (byte[]) ((Pair<byte[], byte[]>) callPair.getFirst())
					.getFirst();
			end = (byte[]) ((Pair<byte[], byte[]>) callPair.getFirst())
					.getSecond();
			for (Map.Entry<HRegionInfo, ServerName> entry : regions.entrySet()) {
				HRegionInfo region = (HRegionInfo) entry.getKey();
				boolean include = false;
				byte[] startKey = region.getStartKey();
				byte[] endKey = region.getEndKey();
				if (Bytes.compareTo(start, startKey) >= 0) {
					if ((Bytes.equals(endKey, HConstants.EMPTY_END_ROW))
							|| (Bytes.compareTo(start, endKey) < 0)) {
						include = true;
					}
				} else {
					if ((!Bytes.equals(end, HConstants.EMPTY_END_ROW))
							&& (Bytes.compareTo(startKey, end) > 0))
						break;
					include = true;
				}

				if (include) {
					ServerName server = (ServerName) entry.getValue();
					List<Pair<byte[], BatchExecCall<T, R>>> callList = (List<Pair<byte[], BatchExecCall<T, R>>>) callsByServer
							.get(server);
					if (callList == null) {
						callList = new ArrayList<Pair<byte[], BatchExecCall<T, R>>>();
						callsByServer.put(server, callList);
					}
					callList.add(new Pair<byte[], BatchExecCall<T, R>>(region
							.getRegionName(), call));
				}
			}
		}

		processBatchExecs(
				HConnectionManager.getConnection(table.getConfiguration()),
				table.getPool(), protocol, callsByServer, serverCallback,
				callback);
	}

	protected static <T extends CoprocessorProtocol, R> void processBatchExecs(
			HConnection connection,
			ExecutorService pool,
			Class<T> protocol,
			Map<ServerName, List<Pair<byte[], BatchExecCall<T, R>>>> callsByServer,
			BatchExecCall.ServerCallback<R> serverCallback,
			final BatchExecCall.ClientCallback<R> callback) throws IOException,
			Throwable {
		Map<String, Future<R>> futures = new TreeMap<String, Future<R>>();

		for (Map.Entry<ServerName, List<Pair<byte[], BatchExecCall<T, R>>>> entry : callsByServer
				.entrySet()) {
			Configuration conf = connection.getConfiguration();
			final ServerName serverName = (ServerName) entry.getKey();
			final BatchExecRPCInvoker<R> invoker = new BatchExecRPCInvoker<R>(
					conf, connection, serverName, protocol, serverCallback);

			@SuppressWarnings("unchecked")
			T instance = (T) Proxy.newProxyInstance(conf.getClassLoader(),
					new Class[] { protocol }, invoker);

			for (Pair<byte[], BatchExecCall<T, R>> callPair : (List<Pair<byte[], BatchExecCall<T, R>>>) entry
					.getValue()) {
				byte[] regionName = (byte[]) callPair.getFirst();
				BatchExecCall<T, R> call = (BatchExecCall<T, R>) callPair
						.getSecond();
				invoker.setRegionName(regionName);
				call.call(instance);
			}
			Future<R> future = pool.submit(new Callable<R>() {
				public R call() throws Exception {
					BatchExecResult result = invoker.realInvoke();
					if (result == null) {
						return null;
					}
					@SuppressWarnings("unchecked")
					R value = (R) result.getValue();
					if (callback != null) {
						callback.update(serverName, result.getRegionNames(),
								value);
					}
					return value;
				}
			});
			futures.put(serverName.getHostAndPort(), future);
		}
		for (Map.Entry<String, Future<R>> e : futures.entrySet())
			try {
				((Future<R>) e.getValue()).get();
			} catch (ExecutionException ee) {
				LOG.warn(
						"Error executing from Region Server "
								+ (String) e.getKey(), ee);
				throw ee.getCause();
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				throw new IOException(
						"Interrupted executing from Region Server "
								+ (String) e.getKey(), ie);
			}
	}
}