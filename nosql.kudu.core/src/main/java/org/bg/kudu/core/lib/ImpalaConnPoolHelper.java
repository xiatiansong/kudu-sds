package org.bg.kudu.core.lib;

import org.bg.kudu.core.ImpalaHelper;
import org.bg.kudu.util.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * impala connection pool帮助类
 *
 * @author xiatiansong
 */
public class ImpalaConnPoolHelper {

    private static final Log LOG = LogFactory.getLog(ImpalaConnPoolHelper.class);

    private GenericObjectPool<ImpalaHelper> impalaConnPool;

    private static volatile ImpalaConnPoolHelper INSTANCE = null;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                INSTANCE.close();
            }
        });
    }

    private ImpalaConnPoolHelper(String url, String username, String password, String driverName, String database) {
        impalaConnPool = new GenericObjectPool<ImpalaHelper>(new ImpalaConnFactory(url, username, password, driverName, database));
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(128);
        config.setMinIdle(5);
        impalaConnPool.setConfig(config);
    }

    public static ImpalaConnPoolHelper getInstance() {
        assert (INSTANCE != null);
        return INSTANCE;
    }

    public synchronized static ImpalaConnPoolHelper getInstance(String url, String username, String password, String driverName, String database) {
        if (INSTANCE == null) {
            INSTANCE = new ImpalaConnPoolHelper(url, username, password, driverName, database);
        }
        return INSTANCE;
    }

    public ImpalaHelper borrow() {
        try {
            return getImpalaConnPool().borrowObject();
        } catch (final Exception ex) {
            LOG.error(LogUtil.getStackTrace(ex), ex);
        }
        return null;
    }

    public void restore(ImpalaHelper session) {
        if (session != null) {
            getImpalaConnPool().returnObject(session);
        }
    }

    public void close() {
        getImpalaConnPool().close();
    }

    public GenericObjectPool<ImpalaHelper> getImpalaConnPool() {
        return impalaConnPool;
    }
}