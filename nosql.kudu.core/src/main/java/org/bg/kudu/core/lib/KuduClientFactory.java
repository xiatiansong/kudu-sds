package org.bg.kudu.core.lib;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;

import java.util.List;

/**
 * 产生 kudu client
 *
 * @author xiatiansong
 */
public class KuduClientFactory extends BasePooledObjectFactory<KuduClient> {

    private List<String> masterInfos;

    public KuduClientFactory(List<String> masterInfos) {
        this.masterInfos = masterInfos;
    }

    public KuduClient create() {
        return createKuduClient();
    }

    public PooledObject<KuduClient> wrap(KuduClient kuduClient) {
        return new DefaultPooledObject<KuduClient>(kuduClient);
    }

    private KuduClient createKuduClient() {
        AsyncKuduClient asyncKuduClient = new AsyncKuduClient.AsyncKuduClientBuilder(masterInfos).build();
        return asyncKuduClient.syncClient();
    }

    public void destroyObject(PooledObject<KuduClient> p) throws Exception {
        p.getObject().close();
        super.destroyObject(p);
    }
}