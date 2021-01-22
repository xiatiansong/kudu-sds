package org.bg.kudu.core.lib;

import org.bg.kudu.util.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;

/**
 * kudu session pool帮助类
 * 操作kuduSessionPool
 *
 * @author xiatiansong
 */
public class KuduSessionPoolHelper {

    private static final Log LOG = LogFactory.getLog(KuduSessionPoolHelper.class);

    private GenericObjectPool<KuduSession> kuduSessionPool;

    private static volatile KuduSessionPoolHelper INSTANCE = null;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                INSTANCE.close();
            }
        });
    }

    private KuduSessionPoolHelper(KuduClient syncKuduClient) {
        kuduSessionPool = new GenericObjectPool<KuduSession>(new KuduSessionFactory(syncKuduClient));
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(256);
        config.setMinIdle(5);
        kuduSessionPool.setConfig(config);
    }

    public static KuduSessionPoolHelper getInstance() {
        assert (INSTANCE != null);
        return INSTANCE;
    }

    public synchronized static KuduSessionPoolHelper getInstance(KuduClient syncKuduClient) {
        if (INSTANCE == null) {
            INSTANCE = new KuduSessionPoolHelper(syncKuduClient);
        }
        return INSTANCE;
    }

    public KuduSession borrow() {
        try {
            return getKuduSessionPool().borrowObject();
        } catch (final Exception ex) {
            LOG.error(LogUtil.getStackTrace(ex), ex);
        }
        return null;
    }

    public void restore(KuduSession session) {
        if (session != null) {
            getKuduSessionPool().returnObject(session);
        }
    }

    public void close() {
        getKuduSessionPool().close();
    }

    public GenericObjectPool<KuduSession> getKuduSessionPool() {
        return kuduSessionPool;
    }
}