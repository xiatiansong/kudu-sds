package org.bg.kudu.core.lib;

import org.bg.kudu.util.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kudu.client.KuduClient;

import java.util.List;

/**
 * kudu client pool帮助类
 * 操作kuduClientPool
 * @author xiatiansong
 *
 */
public class KuduClientPoolHelper {

	private static final Log LOG = LogFactory.getLog(KuduClientPoolHelper.class);

	private GenericObjectPool<KuduClient> kuduClientPool;

	private static volatile KuduClientPoolHelper INSTANCE = null;

	static {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				INSTANCE.close();
			}
		});
	}

	private KuduClientPoolHelper(List<String> masterInfos) {
		kuduClientPool = new GenericObjectPool<KuduClient>(new KuduClientFactory(masterInfos));
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setMaxTotal(128);
		config.setMinIdle(5);
		kuduClientPool.setConfig(config);
	}

	public static KuduClientPoolHelper getInstance()  {
		assert (INSTANCE != null);
		return INSTANCE;
	}

	public synchronized static KuduClientPoolHelper getInstance(List<String> masterInfos) {
		if (INSTANCE == null) {
			INSTANCE = new KuduClientPoolHelper(masterInfos);
		}
		return INSTANCE;
	}

	public KuduClient borrow() {
		try {
			return getKuduClientPool().borrowObject();
		} catch (final Exception ex) {
			LOG.error(LogUtil.getStackTrace(ex), ex);
		}
		return null;
	}

	public void restore(KuduClient kuduClient) {
		if (kuduClient != null) {
			getKuduClientPool().returnObject(kuduClient);
		}
	}

	public void close() {
		getKuduClientPool().close();
	}

	public GenericObjectPool<KuduClient> getKuduClientPool() {
		return kuduClientPool;
	}
}