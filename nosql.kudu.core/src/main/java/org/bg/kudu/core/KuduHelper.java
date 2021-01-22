package org.bg.kudu.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.lumi.bigdata.kudu.core.lib.*;
import org.bg.kudu.core.lib.*;
import org.bg.kudu.util.Constants;
import org.bg.kudu.util.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.apache.kudu.shaded.com.google.common.cache.Cache;
import org.apache.kudu.shaded.com.google.common.cache.CacheBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * kudu操作类
 *
 * @author xiatiansong
 */
public class KuduHelper implements Closeable {

    private static final Log LOG = LogFactory.getLog(KuduHelper.class);

    /**
     * 使用本地緩存方式存储KuduTable，包含表schema,本地缓存失效时间为5分钟
     **/
    private static final Cache<String, KuduTable> KUDUTABLE_MAPS = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(300, TimeUnit.SECONDS).build();

    private static KuduHelper INSTANCE;
    private KuduClient syncKuduClient;
    private AsyncKuduClient asyncKuduClient;
    private final List<String> masterInfos;
    /**
     * static {
     * Runtime.getRuntime().addShutdownHook(new Thread() {
     *
     * @Override public void run() {
     * try {
     * INSTANCE.close();
     * } catch (IOException e) {
     * LOG.error(LogUtil.getStackTrace(e), e);
     * }
     * }
     * });
     * }
     **/

    public static KuduHelper getInstance() {
        assert (INSTANCE != null);
        return INSTANCE;
    }

    /**
     * 根据kudu master 集群ip初始化客户端
     *
     * @param masterInfos
     * @return
     */
    public synchronized static KuduHelper getInstance(List<String> masterInfos) {
        if (INSTANCE == null) {
            INSTANCE = new KuduHelper(masterInfos);
        }
        return INSTANCE;
    }

    public synchronized static void stop() {
        if (INSTANCE != null) {
            INSTANCE = null;
        }
    }

    /**
     * 根据kudu master 集群ip初始化客户端
     *
     * @param masterInfos
     */
    private KuduHelper(List<String> masterInfos) {
        this.masterInfos = masterInfos;
        asyncKuduClient = new AsyncKuduClient.AsyncKuduClientBuilder(masterInfos).build();
        syncKuduClient = asyncKuduClient.syncClient();
        KuduSessionPoolHelper.getInstance(syncKuduClient);
        KuduClientPoolHelper.getInstance(masterInfos);
    }

    private void reInit() {
        LOG.warn("reInit asyncKuduClient and syncKuduClient.!!!!!!!!!!");
        asyncKuduClient = new AsyncKuduClient.AsyncKuduClientBuilder(this.masterInfos).build();
        syncKuduClient = asyncKuduClient.syncClient();
        KuduSessionPoolHelper.getInstance(syncKuduClient);
        KuduClientPoolHelper.getInstance(masterInfos);
    }

    @Override
    public void close() throws IOException {
        try {
            LOG.warn("KuduHelper.close()");
            //syncKuduClient.close();
            asyncKuduClient.close();
        } catch (Exception e) {
            LOG.error(LogUtil.getStackTrace(e), e);
        }
    }

    /**
     * 获取kudu table
     *
     * @param tableName
     * @return
     */
    public KuduTable getKuduTable(final String tableName) {
        try {
            KuduTable kuduTable = KUDUTABLE_MAPS.get(tableName, new Callable<KuduTable>() {
                @Override
                public KuduTable call() throws KuduException {
                    try {
                        return syncKuduClient.openTable(tableName);
                    } catch (IllegalStateException e) {
                        if (e.getMessage().equals("Cannot proceed, the client has already been closed")) {
                            reInit();
                        }
                    }
                    return syncKuduClient.openTable(tableName);
                }
            });
            return kuduTable;
        } catch (ExecutionException e) {
            LOG.error(LogUtil.getAllStackTrace(e));
        }
        return null;
    }



    public Schema getTableSchema(String tableName) {
        return getKuduTable(tableName).getSchema();
    }

    /**
     * cud 批量操作
     *
     * @param tableName
     * @param dataList
     */
    public void batchCudOperate(String tableName, KuduOp op, List<Map<String, Object>> dataList) {
        KuduSession kuduSession = null;
        try {
            KuduTable kuduTable = getKuduTable(tableName);
            kuduSession = KuduSessionPoolHelper.getInstance().borrow();
            for (Map<String, Object> data : dataList) {
                Operation operation = null;
                switch (op) {
                    case INSERT:
                        operation = kuduTable.newInsert();
                        break;
                    case UPDATE:
                        operation = kuduTable.newUpdate();
                        break;
                    case UPSERT:
                        operation = kuduTable.newUpsert();
                        break;
                    case DELETE:
                        operation = kuduTable.newDelete();
                        break;
                    default:
                        break;
                }
                if(operation == null){
                    throw new RuntimeException("Error Kudu Operation " + op);
                }
                PartialRow row = operation.getRow();
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    KuduDataUtil.addColumnValue(row, entry.getKey(), getTableSchema(tableName), entry.getValue());
                }
                kuduSession.apply(operation);
            }
            kuduSession.flush();
        } catch (Exception e) {
            if (e instanceof org.apache.kudu.client.KuduException) {
                KUDUTABLE_MAPS.invalidate(tableName);
            }
            if (KuduOp.INSERT == op) {
                this.batchCudOperate(tableName, KuduOp.UPSERT, dataList);
            } else {
                LOG.error(LogUtil.getStackTrace(e), e);
                throw new RuntimeException("KuduHelper.batchCudOperate error:" + e.getMessage());
            }
        } finally {
            KuduSessionPoolHelper.getInstance().restore(kuduSession);
        }
    }

    /**
     * crud 操作
     *
     * @param tableName
     * @param data
     */
    public void cudOperate(String tableName, KuduOp op, Map<String, Object> data) {
        KuduSession kuduSession = null;
        try {
            KuduTable kuduTable = getKuduTable(tableName);
            Operation operation = null;
            switch (op) {
                case INSERT:
                    operation = kuduTable.newInsert();
                    break;
                case UPDATE:
                    operation = kuduTable.newUpdate();
                    break;
                case UPSERT:
                    operation = kuduTable.newUpsert();
                    break;
                case DELETE:
                    operation = kuduTable.newDelete();
                    break;
                default:
                    break;
            }
            if(operation == null){
                throw new RuntimeException("Error Kudu Operation " + op);
            }
            PartialRow row = operation.getRow();
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                KuduDataUtil.addColumnValue(row, entry.getKey(), getTableSchema(tableName), entry.getValue());
            }
            kuduSession = KuduSessionPoolHelper.getInstance().borrow();
            kuduSession.apply(operation);
            kuduSession.flush();
        } catch (Exception e) {
            if (KuduOp.INSERT == op) {
                this.cudOperate(tableName, KuduOp.UPSERT, data);
            } else {
                LOG.error(LogUtil.getStackTrace(e), e);
                throw new RuntimeException("KuduHelper.cudOperate error:" + e.getMessage());
            }
        } finally {
            KuduSessionPoolHelper.getInstance().restore(kuduSession);
        }
    }

    /**
     * 通过主键开始和结束参数scan
     *
     * @param tableName
     * @param startParam
     * @param endParam
     * @param limit
     * @return
     */
    public List<Map<String, Object>> scanDataByStartEndParam(String tableName, Map<String, Object> startParam, Map<String, Object> endParam, int limit, boolean ordered,
                                                             List<ColumnSchema> columnList) {
        KuduScanner ks = null;
        KuduClient kuduClient = null;
        List<Map<String, Object>> columnDatas = Lists.newArrayList();
        try {
            kuduClient = KuduClientPoolHelper.getInstance().borrow();
            KuduTable kuduTable = getKuduTable(tableName);
            KuduScannerBuilder builder = kuduClient.newScannerBuilder(kuduTable);
            //限制返回的数据数量
            if (limit > 0) {
                builder.limit(limit);
            } else {
                builder.limit(Constants.KuduConstants.SCAN_LIMIT_NUM);
            }
            //设置全局扫描
            if (ordered) {
                builder.setFaultTolerant(true);
            }
            //设置开始和结束 primary key
            builder.lowerBound(KuduDataUtil.getPartialRow(startParam, kuduTable, true));
            builder.exclusiveUpperBound(KuduDataUtil.getPartialRow(endParam, kuduTable, false));
            ks = builder.build();
            while (ks.hasMoreRows()) {
                this.iterateResultSet(ks.nextRows(), columnDatas, columnList);
            }
        } catch (Exception e) {
            LOG.error(LogUtil.getStackTrace(e), e);
            throw new RuntimeException(e);
        } finally {
            try {
                KuduClientPoolHelper.getInstance().restore(kuduClient);
                if (ks != null) {
                    ks.close();
                }
            } catch (KuduException e) {
                LOG.error(LogUtil.getStackTrace(e), e);
            }
        }
        return columnDatas;
    }

    /**
     * 通过Filter获取单条数据
     *
     * @param tableName
     * @param filters
     * @return
     */
    public Map<String, Object> getByFilter(String tableName, List<BaseFilter> filters, List<ColumnSchema> columnList) {
        List<Map<String, Object>> rsList = scanDataByFilter(tableName, filters, 1, false, columnList);
        return rsList.size() == 0 ? null : rsList.get(0);
    }

    /**
     * 通过filter查询数据
     *
     * @param tableName
     * @param filters
     * @return
     */
    public List<Map<String, Object>> scanDataByFilter(String tableName, List<BaseFilter> filters, int limit, boolean ordered, List<ColumnSchema> columnList) {
        KuduScanner ks = null;
        KuduClient kuduClient = null;
        List<Map<String, Object>> columnDatas = Lists.newArrayList();
        try {
            kuduClient = KuduClientPoolHelper.getInstance().borrow();
            KuduTable kuduTable = getKuduTable(tableName);
            KuduScannerBuilder builder = kuduClient.newScannerBuilder(kuduTable);
            //限制返回的数据数量
            if (limit > 0) {
                builder.limit(limit);
            } else {
                builder.limit(Constants.KuduConstants.SCAN_LIMIT_NUM);
            }
            //设置全局扫描
            if (ordered) {
                builder.setFaultTolerant(true);
            }
            //设置filter
            if (filters != null && !filters.isEmpty()) {
                for (BaseFilter aFilter : filters) {
                    builder.addPredicate(KuduDataUtil.getPredicate(aFilter, kuduTable));
                }
            }
            ks = builder.build();
            while (ks.hasMoreRows()) {
                this.iterateResultSet(ks.nextRows(), columnDatas, columnList);
            }
        } catch (Exception e) {
            LOG.error(LogUtil.getStackTrace(e), e);
            throw new RuntimeException(e);
        } finally {
            try {
                KuduClientPoolHelper.getInstance().restore(kuduClient);
                if (ks != null) {
                    ks.close();
                }
            } catch (KuduException e) {
                LOG.error(LogUtil.getStackTrace(e), e);
            }
        }
        return columnDatas;
    }

    /**
     * 遍历RowResult
     *
     * @param rit
     * @return
     * @throws IOException
     * @throws ParseException
     */
    private void iterateResultSet(RowResultIterator rit, List<Map<String, Object>> columnDatas, List<ColumnSchema> columnList) {
        while (rit.hasNext()) {
            Map<String, Object> dataMap = Maps.newHashMap();
            RowResult row = rit.next();
            for (ColumnSchema cs : columnList) {
                KuduDataUtil.addResultValue(row, cs, dataMap);
            }
            columnDatas.add(dataMap);
        }
    }
}