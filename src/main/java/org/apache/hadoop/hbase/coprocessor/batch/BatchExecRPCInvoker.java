package org.apache.hadoop.hbase.coprocessor.batch;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public class BatchExecRPCInvoker<R> implements InvocationHandler {
	private Configuration conf;
	private final HConnection connection;
	private final ServerName serverName;
	private Class<? extends CoprocessorProtocol> protocol;
	private BatchExecCall.ServerCallback<R> serverCallback;
	private List<BatchExec> execList;
	private byte[] regionName;

	public BatchExecRPCInvoker(Configuration conf, HConnection connection,
			ServerName serverName,
			Class<? extends CoprocessorProtocol> protocol,
			BatchExecCall.ServerCallback<R> serverCallback) {
		this.conf = conf;
		this.connection = connection;
		this.serverName = serverName;
		this.protocol = protocol;
		this.serverCallback = serverCallback;
		this.execList = new ArrayList<BatchExec>();
	}

	public void setRegionName(byte[] regionName) {
		this.regionName = regionName;
	}

	public Object invoke(Object instance, Method method, Object[] args)
			throws Throwable {
		if (this.regionName != null) {
			this.execList.add(new BatchExec(this.conf, this.regionName,
					this.protocol, method, args));
		}

		return null;
	}

	public BatchExecResult realInvoke() throws Exception {
		if (!this.execList.isEmpty()) {
			String id = UUID.randomUUID().toString();
			return this.connection.getHRegionConnection(
					this.serverName.getHostname(), this.serverName.getPort())
					.execBatchCoprocessor(id, this.execList,
							this.serverCallback);
		}

		return null;
	}
}