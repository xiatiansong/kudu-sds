package org.bg.kudu.core.lib;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

/**
 * 产生 kudu session
 * @author xiatiansong
 *
 */
public class KuduSessionFactory extends BasePooledObjectFactory<KuduSession> {

	private KuduClient syncKuduClient;

	public KuduSessionFactory(KuduClient syncKuduClient) {
		this.syncKuduClient = syncKuduClient;
	}

	public KuduSession create() {
		return createKuduSession();
	}

	public PooledObject<KuduSession> wrap(KuduSession kuduSession) {
		return new DefaultPooledObject<KuduSession>(kuduSession);
	}

	private KuduSession createKuduSession() {
		KuduSession session = syncKuduClient.newSession();
		session.setFlushMode(FlushMode.MANUAL_FLUSH);
		session.setMutationBufferSpace(10000);
		session.setTimeoutMillis(1000L * 60L);
		return session;
	}

	public void destroyObject(PooledObject<KuduSession> p) throws Exception {
		p.getObject().flush();
		p.getObject().close();
		super.destroyObject(p);
	}
}