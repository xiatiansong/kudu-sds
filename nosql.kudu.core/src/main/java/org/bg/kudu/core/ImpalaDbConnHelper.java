package org.bg.kudu.core;

import org.bg.kudu.util.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * impala链接管理
 *
 * @author xiatiansong
 */
public class ImpalaDbConnHelper implements Closeable {

    private static final Log LOG = LogFactory.getLog(ImpalaDbConnHelper.class);

    /**
     * ImpalaHelper实例缓存，一个database一个ImpalaHelper
     **/
    private static final ConcurrentHashMap<String, ImpalaHelper> IMPALA_HELPER_MAP = new ConcurrentHashMap<String, ImpalaHelper>();

    private static ImpalaDbConnHelper INSTANCE;

    private String url;

    private String driverName;

    private String username;

    private String password;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    INSTANCE.close();
                } catch (IOException e) {
                    LOG.error("error close connection");
                }
            }
        });
    }

    public static ImpalaDbConnHelper getInstance() {
        assert (INSTANCE != null);
        return INSTANCE;
    }

    /**
     * 根据kudu master 集群ip初始化客户端
     *
     * @param url
     * @param driverName
     * @return
     */
    public synchronized static ImpalaDbConnHelper getInstance(String url, String username, String password, String driverName) {
        if (INSTANCE == null) {
            INSTANCE = new ImpalaDbConnHelper(url, username, password, driverName);
            INSTANCE.getImpalaHelper("default");
        }
        return INSTANCE;
    }

    public synchronized static void stop() {
        if (INSTANCE != null) {
            INSTANCE = null;
        }
    }

    private ImpalaDbConnHelper(String url, String username, String password, String driverName) {
        this.url = url;
        this.driverName = driverName;
        this.username = username;
        this.password = password;
    }

    public ImpalaHelper getImpalaHelper(String database) {
        ImpalaHelper impalaHelper = IMPALA_HELPER_MAP.get(database);
        if (impalaHelper == null) {
            impalaHelper = ImpalaHelper.getNewInstance(this.url, this.username, this.password, this.driverName, database);
            IMPALA_HELPER_MAP.put(database, impalaHelper);
        }
        return impalaHelper;
    }

    public ImpalaHelper getNewImpalaHelper(String database) {
        ImpalaHelper impalaHelper = IMPALA_HELPER_MAP.get(database);
        if (impalaHelper != null) {
            try {
                impalaHelper.close();
                impalaHelper = ImpalaHelper.getNewInstance(this.url, this.username, this.password, this.driverName, database);
                IMPALA_HELPER_MAP.put(database, impalaHelper);
            } catch (IOException e) {
                LOG.error(LogUtil.getAllStackTrace(e));
            }
        }
        return impalaHelper;
    }

    @Override
    public void close() throws IOException {
        for (ImpalaHelper impalaHelper : IMPALA_HELPER_MAP.values()) {
            impalaHelper.close();
        }
    }
}