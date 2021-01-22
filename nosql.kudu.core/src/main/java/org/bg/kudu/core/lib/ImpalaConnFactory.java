package org.bg.kudu.core.lib;

import org.bg.kudu.core.ImpalaHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * 产生 kudu session
 *
 * @author xiatiansong
 */
public class ImpalaConnFactory extends BasePooledObjectFactory<ImpalaHelper> {

    private static final Log LOG = LogFactory.getLog(ImpalaConnFactory.class);

    private String url;
    private String username;
    private String password;
    private String driverName;
    private String database;

    public ImpalaConnFactory(String url, String username, String password, String driverName, String database) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.driverName = driverName;
        this.database = database;
    }

    public ImpalaHelper create() {
        return createImpalaConn();
    }

    public PooledObject<ImpalaHelper> wrap(ImpalaHelper ih) {
        return new DefaultPooledObject<ImpalaHelper>(ih);
    }

    private ImpalaHelper createImpalaConn() {
        return ImpalaHelper.getNewInstance(this.url, this.username, this.password, this.driverName, this.database);
    }

    public void destroyObject(PooledObject<ImpalaHelper> p) throws Exception {
        p.getObject().close();
        super.destroyObject(p);
    }
}