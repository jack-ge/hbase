package org.apache.hadoop.hbase.coprocessor.batch;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.io.Writable;

public abstract interface BatchExecCall<T, R> {
	public abstract R call(T paramT) throws IOException;

	public static abstract interface ClientCallback<R> {
		public abstract void update(ServerName paramServerName,
				List<byte[]> paramList, R paramR) throws IOException;
	}

	public static abstract interface ServerCallback<R> extends Writable {
		public abstract void update(byte[] paramArrayOfByte, R paramR)
				throws IOException;

		public abstract void init();

		public abstract R getResult();
	}
}