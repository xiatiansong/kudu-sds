package org.bg.kudu.client;

import com.alibaba.dcm.DnsCacheManipulator;
import com.google.common.collect.Lists;
import org.bg.kudu.annotation.MissingAnnotationException;
import org.bg.kudu.annotation.Table;
import org.bg.kudu.client.helper.BeanHelper;
import org.bg.kudu.model.DmlResult;
import org.bg.kudu.model.Predicates;
import org.bg.kudu.core.ImpalaOptHelper;
import org.bg.kudu.core.KuduHelper;
import org.bg.kudu.core.KuduOptHelper;
import org.bg.kudu.core.lib.KuduOp;
import org.bg.kudu.model.KuduPage;
import org.bg.kudu.model.KuduPageParams;
import org.bg.kudu.model.KuduPageParamsDto;
import org.bg.kudu.util.Constants;
import org.bg.kudu.util.LogUtil;
import org.bg.kudu.util.PropertiesUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

/**
 * <p>
 * 提供Kudu对Map数据类型参数的一些操作
 * </p>
 * <bean id="kuduDataTemplate" class="org.bg.kudu.client.KuduDataTemplate">
 * <constructor-arg name="kuduAddr" value="http://10.53.2.7:30006"/>
 * </bean>
 *
 * @author xiatiansong
 */
public class KuduDataTemplate implements Closeable {

    private static final Log LOG = LogFactory.getLog(KuduDataTemplate.class);

    public KuduDataTemplate() {
        super();
        try {
            DnsCacheManipulator.loadDnsCacheConfig();
            readPropertyToInit();
        } catch (Exception e) {
            LOG.error(LogUtil.getStackTrace(e), e);
        }
    }

    public KuduDataTemplate(String url, String driverName, String username, String password, String masterHosts) {
        super();
        try {
            DnsCacheManipulator.loadDnsCacheConfig();
            initInstance(url, driverName, username, password, masterHosts);
        } catch (Exception e) {
            LOG.error(LogUtil.getStackTrace(e), e);
        }
    }

    private void readPropertyToInit() {
        PropertiesUtil prop = PropertiesUtil.getInstance();
        prop.loadProperty("kudu-server.properties");
        String url = prop.getProperty("impala.connection.url");
        String driverName = prop.getProperty("impala.jdbc.driver.class.name");
        String username = prop.getProperty("impala.connection.username");
        String password = prop.getProperty("impala.connection.password");
        String masterHosts = prop.getProperty("kudu.server.master.hosts");
        initInstance(url, driverName, username, password, masterHosts);
    }

    private void initInstance(String url, String driverName, String username, String password, String masterHosts) {
        List<String> masterInfos = Arrays.asList(masterHosts.split(","));
        KuduHelper.getInstance(masterInfos);
        ImpalaOptHelper.getInstance(url, driverName, username, password);
    }

    private <T> List<Map<String, Object>> getMapNodesFromBean(List<T> rows) throws Exception {
        List<Map<String, Object>> maps = Lists.newArrayList();
        for (T obj : rows) {
            Map<String, Object> map = BeanHelper.toMap(obj);
            maps.add(map);
        }
        return maps;
    }

    private <T> List<Map<String, Object>> getPrimaryMapNodesFromBean(List<T> entityList) throws Exception {
        List<Map<String, Object>> an = Lists.newArrayList();
        for (T obj : entityList) {
            Map<String, Object> map = BeanHelper.toPrimaryKeyMap(obj);
            an.add(map);
        }
        return an;
    }

    private String getTableName(String dbName, String tableName) {
        return dbName + "." + tableName;
    }

    /**
     * 批量插入数据
     */
    public void insertRowList(String dbName, String tableName, List<Map<String, Object>> rows) throws Exception {
        KuduOptHelper.cudOperates(getTableName(dbName, tableName), KuduOp.INSERT, rows);
    }

    /**
     * 批量插入数据
     */
    public <T> void insertObjectList(String dbName, String tableName, List<T> rows) throws Exception {
        KuduOptHelper.cudOperates(getTableName(dbName, tableName), KuduOp.INSERT, getMapNodesFromBean(rows));
    }

    /**
     * 批量插入数据
     */
    public <T> void insertObjectList(List<T> rows) throws Exception {
        if (null != rows && rows.size() > 0) {
            T firstObj = rows.get(0);
            Table tableAnnotation = firstObj.getClass().getAnnotation(Table.class);
            if (null != tableAnnotation) {
                String tableName = tableAnnotation.name();
                String dbName = tableAnnotation.database();
                KuduOptHelper.cudOperates(getTableName(dbName, tableName), KuduOp.INSERT, getMapNodesFromBean(rows));
            } else {
                throw new MissingAnnotationException("无法获取表注解.");
            }
        }
    }

    /**
     * 插入单行数据
     */
    public void insertRow(String dbName, String tableName, Map<String, Object> row) {
        KuduOptHelper.cudOperate(getTableName(dbName, tableName), KuduOp.INSERT, row);
    }

    /**
     * 插入单行数据
     */
    public <T> void insertObject(String dbName, String tableName, T row) throws Exception {
        Map<String, Object> map = BeanHelper.toMap(row);
        KuduOptHelper.cudOperate(getTableName(dbName, tableName), KuduOp.INSERT, map);
    }

    /**
     * 插入单行数据
     */
    public <T> void insertObject(T row) throws Exception {
        Map<String, Object> map = BeanHelper.toMap(row);
        Table tableAnnotation = row.getClass().getAnnotation(Table.class);
        if (null != tableAnnotation) {
            String tableName = tableAnnotation.name();
            String dbName = tableAnnotation.database();
            KuduOptHelper.cudOperate(getTableName(dbName, tableName), KuduOp.INSERT, map);
        } else {
            throw new MissingAnnotationException("无法获取表注解.");
        }
    }

    /**
     * 批量删除数据
     */
    public void deleteRowList(String dbName, String tableName, List<Map<String, Object>> rows) throws Exception {
        KuduOptHelper.cudOperates(getTableName(dbName, tableName), KuduOp.DELETE, rows);
    }

    /**
     * 批量删除数据
     */
    public <T> void deleteObjectList(String dbName, String tableName, List<T> entityList) throws Exception {
        KuduOptHelper.cudOperates(getTableName(dbName, tableName), KuduOp.DELETE, getPrimaryMapNodesFromBean(entityList));
    }

    /**
     * 批量删除数据
     */
    public <T> void deleteObjectList(List<T> entityList) throws Exception {
        if (null != entityList && entityList.size() > 0) {
            T firstObj = entityList.get(0);
            Table tableAnnotation = firstObj.getClass().getAnnotation(Table.class);
            if (null != tableAnnotation) {
                String tableName = tableAnnotation.name();
                String dbName = tableAnnotation.database();
                KuduOptHelper.cudOperates(getTableName(dbName, tableName), KuduOp.DELETE, getPrimaryMapNodesFromBean(entityList));
            } else {
                throw new MissingAnnotationException("无法获取表注解.");
            }
        }
    }

    /**
     * 删除单行数据
     */
    public void deleteRow(String dbName, String tableName, Map<String, Object> row) throws Exception {
        KuduOptHelper.cudOperate(getTableName(dbName, tableName), KuduOp.DELETE, row);
    }

    /**
     * 删除单行数据
     */
    public <T> void deleteObject(String dbName, String tableName, T row) throws Exception {
        Map<String, Object> map = BeanHelper.toMap(row);
        KuduOptHelper.cudOperate(getTableName(dbName, tableName), KuduOp.DELETE, map);
    }

    /**
     * 删除单行数据
     */
    public <T> void deleteObject(T row) throws Exception {
        Map<String, Object> map = BeanHelper.toPrimaryKeyMap(row);
        Table tableAnnotation = row.getClass().getAnnotation(Table.class);
        if (null != tableAnnotation) {
            String tableName = tableAnnotation.name();
            String dbName = tableAnnotation.database();
            KuduOptHelper.cudOperate(getTableName(dbName, tableName), KuduOp.DELETE, map);
        } else {
            throw new MissingAnnotationException("无法获取表注解.");
        }
    }

    /**
     * 批量upsert数据
     */
    public void upsertRowList(String dbName, String tableName, List<Map<String, Object>> rows) throws Exception {
        KuduOptHelper.cudOperates(getTableName(dbName, tableName), KuduOp.UPSERT, rows);
    }

    /**
     * 批量upsert数据
     */
    public <T> void upsertObjectList(String dbName, String tableName, List<T> rows) throws Exception {
        KuduOptHelper.cudOperates(getTableName(dbName, tableName), KuduOp.UPSERT, getMapNodesFromBean(rows));
    }

    /**
     * 批量upsert数据
     */
    public <T> void upsertObjectList(List<T> rows) throws Exception {
        if (null != rows && rows.size() > 0) {
            T firstObj = rows.get(0);
            Table tableAnnotation = firstObj.getClass().getAnnotation(Table.class);
            if (null != tableAnnotation) {
                String tableName = tableAnnotation.name();
                String dbName = tableAnnotation.database();
                KuduOptHelper.cudOperates(getTableName(dbName, tableName), KuduOp.UPSERT, getMapNodesFromBean(rows));
            } else {
                throw new MissingAnnotationException("无法获取表注解.");
            }
        }
    }

    /**
     * upsert单行数据
     */
    public void upsertRow(String dbName, String tableName, Map<String, Object> row) throws Exception {
        KuduOptHelper.cudOperate(getTableName(dbName, tableName), KuduOp.UPSERT, row);
    }

    /**
     * upsert单行数据
     */
    public <T> void upsertObject(String dbName, String tableName, T row) throws Exception {
        Map<String, Object> map = BeanHelper.toMap(row);
        KuduOptHelper.cudOperate(getTableName(dbName, tableName), KuduOp.UPSERT, map);
    }

    /**
     * upsert单行数据
     */
    public <T> void upsertObject(T row) throws Exception {
        Map<String, Object> map = BeanHelper.toMap(row);
        Table tableAnnotation = row.getClass().getAnnotation(Table.class);
        if (null != tableAnnotation) {
            String tableName = tableAnnotation.name();
            String dbName = tableAnnotation.database();
            KuduOptHelper.cudOperate(getTableName(dbName, tableName), KuduOp.UPSERT, map);
        } else {
            throw new MissingAnnotationException("无法获取表注解.");
        }
    }

    /**
     * 通过主键获取单行数据
     */
    public Map<String, Object> getRow(String dbName, String tableName, Map<String, Object> row) throws Exception {
        return KuduOptHelper.get(getTableName(dbName, tableName), row);
    }

    /**
     * 通过BeanUtil获取单行数据，返回对象类型
     */
    public <T> T getRow(String dbName, String tableName, Map<String, Object> row, Class<T> clazz) throws Exception {
        Map<String, Object> data = KuduOptHelper.get(getTableName(dbName, tableName), row);
        return BeanHelper.toObject(data, clazz);
    }

    /**
     * 通过BeanUtil获取单行数据，返回对象类型
     */
    public <T> T getRow(T rowObj) throws Exception {
        Map<String, Object> row = BeanHelper.toPrimaryKeyMap(rowObj);
        Table tableAnnotation = rowObj.getClass().getAnnotation(Table.class);
        if (null != tableAnnotation) {
            String tableName = tableAnnotation.name();
            String dbName = tableAnnotation.database();
            Map<String, Object> data = KuduOptHelper.get(getTableName(dbName, tableName), row);
            return BeanHelper.toObject(data, (Class<T>) rowObj.getClass());
        } else {
            throw new MissingAnnotationException("无法获取表注解.");
        }
    }

    public List<Map<String, Object>> scanByPredicates(String dbName, String tableName, Map<String, Object> params) throws Exception {
        return scanByPredicates(dbName, tableName, params, Constants.MAX_LIMIT, false);
    }

    public List<Map<String, Object>> scanByPredicates(String dbName, String tableName, Map<String, Object> params, int limit) throws Exception {
        return scanByPredicates(dbName, tableName, params, limit, false);
    }

    public List<Map<String, Object>> scanByPredicates(String dbName, String tableName, Map<String, Object> params, boolean ordered) throws Exception {
        return scanByPredicates(dbName, tableName, params, Constants.MAX_LIMIT, ordered);
    }

    public List<Map<String, Object>> scanByPredicates(String dbName, String tableName, List<Predicates> predicateList) throws Exception {
        return scanByPredicates(dbName, tableName, predicateList, Constants.MAX_LIMIT, false);
    }

    public List<Map<String, Object>> scanByPredicates(String dbName, String tableName, List<Predicates> predicateList, boolean ordered) throws Exception {
        return scanByPredicates(dbName, tableName, predicateList, Constants.MAX_LIMIT, ordered);
    }

    public List<Map<String, Object>> scanByPredicates(String dbName, String tableName, List<Predicates> predicateList, int limit) throws Exception {
        return scanByPredicates(dbName, tableName, predicateList, limit, false);
    }

    public List<Map<String, Object>> scanByPredicates(String dbName, String tableName, List<Predicates> predicateList, int limit, boolean ordered) throws Exception {
        Map<String, Object> params = predictToMap(predicateList);
        return scanByPredicates(dbName, tableName, params, limit, ordered);
    }

    /**
     * 任意条件组合进行查询数据，返回Map
     * params中除了可以设置key、value (查询条件为key = value)，也可以是 key、[left,right] 代表 left <= key <= right
     */
    public List<Map<String, Object>> scanByPredicates(String dbName, String tableName, Map<String, Object> params, int limit, boolean ordered) throws Exception {
        return KuduOptHelper.scanPredicates(getTableName(dbName, tableName), mapKeyToLowerCase(params), limit, ordered);
    }


    public <T> List<T> scanByPredicates(String dbName, String tableName, Map<String, Object> params, Class<T> clazz) throws Exception {
        return scanByPredicates(dbName, tableName, params, clazz, Constants.MAX_LIMIT, false);
    }

    public <T> List<T> scanByPredicates(String dbName, String tableName, Map<String, Object> params, Class<T> clazz, int limit) throws Exception {
        return scanByPredicates(dbName, tableName, params, clazz, limit, false);
    }

    public <T> List<T> scanByPredicates(String dbName, String tableName, Map<String, Object> params, Class<T> clazz, boolean ordered) throws Exception {
        return scanByPredicates(dbName, tableName, params, clazz, Constants.MAX_LIMIT, ordered);
    }

    public <T> List<T> scanByPredicates(String dbName, String tableName, List<Predicates> predicateList, Class<T> clazz) throws Exception {
        return scanByPredicates(dbName, tableName, predicateList, clazz, Constants.MAX_LIMIT);
    }

    public <T> List<T> scanByPredicates(String dbName, String tableName, List<Predicates> predicateList, Class<T> clazz, int limit) throws Exception {
        return scanByPredicates(dbName, tableName, predicateList, clazz, limit, false);
    }

    public <T> List<T> scanByPredicates(String dbName, String tableName, List<Predicates> predicateList, Class<T> clazz, boolean ordered) throws Exception {
        return scanByPredicates(dbName, tableName, predicateList, clazz, Constants.MAX_LIMIT, ordered);
    }

    public <T> List<T> scanByPredicates(String dbName, String tableName, List<Predicates> predicateList, Class<T> clazz, int limit, boolean ordered) throws Exception {
        Map<String, Object> params = predictToMap(predicateList);
        return scanByPredicates(dbName, tableName, params, clazz, limit, ordered);
    }

    /**
     * 任意条件组合进行查询数据，返回对象类型
     * params中除了可以设置key、value (查询条件为key = value)，也可以是 key、[left,right] 代表 left <= key <= right
     */
    public <T> List<T> scanByPredicates(String dbName, String tableName, Map<String, Object> params, Class<T> clazz, int limit, boolean ordered) throws Exception {
        List<Map<String, Object>> dataList = KuduOptHelper.scanPredicates(getTableName(dbName, tableName), mapKeyToLowerCase(params), limit, ordered);
        return extractMapNodeToBean(dataList, clazz);
    }

    public <T> List<T> scanByPredicates(Map<String, Object> params, Class<T> clazz) throws Exception {
        return scanByPredicates(params, clazz, Constants.MAX_LIMIT, false);
    }

    public <T> List<T> scanByPredicates(Map<String, Object> params, Class<T> clazz, int limit) throws Exception {
        return scanByPredicates(params, clazz, limit, false);
    }

    public <T> List<T> scanByPredicates(Map<String, Object> params, Class<T> clazz, boolean ordered) throws Exception {
        return scanByPredicates(params, clazz, Constants.MAX_LIMIT, ordered);
    }

    public <T> List<T> scanByPredicates(List<Predicates> predicateList, Class<T> clazz) throws Exception {
        return scanByPredicates(predicateList, clazz, Constants.MAX_LIMIT);
    }

    public <T> List<T> scanByPredicates(List<Predicates> predicateList, Class<T> clazz, int limit) throws Exception {
        return scanByPredicates(predicateList, clazz, limit, false);
    }

    public <T> List<T> scanByPredicates(List<Predicates> predicateList, Class<T> clazz, boolean ordered) throws Exception {
        return scanByPredicates(predicateList, clazz, Constants.MAX_LIMIT, ordered);
    }

    public <T> List<T> scanByPredicates(List<Predicates> predicateList, Class<T> clazz, int limit, boolean ordered) throws Exception {
        Map<String, Object> params = predictToMap(predicateList);
        return scanByPredicates(params, clazz);
    }

    /**
     * 任意条件组合进行查询数据，返回对象类型
     * params中除了可以设置key、value (查询条件为key = value)，也可以是 key、[left,right] 代表 left <= key <= right
     */
    public <T> List<T> scanByPredicates(Map<String, Object> params, Class<T> clazz, int limit, boolean ordered) throws Exception {
        Table tableAnnotation = clazz.getAnnotation(Table.class);
        if (null != tableAnnotation) {
            params = mapKeyToLowerCase(params);
            String tableName = tableAnnotation.name();
            String dbName = tableAnnotation.database();
            List<Map<String, Object>> dataList = KuduOptHelper.scanPredicates(getTableName(dbName, tableName), mapKeyToLowerCase(params), limit, ordered);
            return extractMapNodeToBean(dataList, clazz);
        } else {
            throw new MissingAnnotationException("无法获取表注解.");
        }
    }

    /**
     * kudu 分页
     *
     * @param dbName
     * @param tableName
     * @param kuduPageParams
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> KuduPage<T> scanByPage(String dbName, String tableName, KuduPageParams kuduPageParams, Class<T> clazz) throws Exception {
        KuduPage<T> kuduPage = new KuduPage<>();
        int querySize = kuduPageParams.getPageSize() + 1;
        KuduPageParamsDto kppd = new KuduPageParamsDto();
        kppd.setCurrentPageParams(kuduPageParams);
        List<T> result = scanByPredicates(dbName, tableName, kuduPageParams.getParams(), clazz, querySize, false);
        if (null != result && result.size() == querySize) {
            kuduPage.setResult(result.subList(0, kuduPageParams.getPageSize()));
            T last = result.get(result.size() - 1);
            KuduPageParams nkpp = getNextPageParam(kuduPageParams, last);
            kppd.setNextPageParams(nkpp);
        } else {
            if (null != result) {
                kuduPage.setResult(result);
            }
            kppd.setLastPage(true);
        }
        kuduPage.setPageInfo(kppd);
        return kuduPage;
    }

    /**
     * kudu 分页
     *
     * @param dbName
     * @param tableName
     * @param kuduPageParams
     * @return
     * @throws Exception
     */
    public KuduPage<Map<String, Object>> scanByPage(String dbName, String tableName, KuduPageParams kuduPageParams) throws Exception {
        KuduPage<Map<String, Object>> kuduPage = new KuduPage<>();
        int querySize = kuduPageParams.getPageSize() + 1;
        KuduPageParamsDto kppd = new KuduPageParamsDto();
        kppd.setCurrentPageParams(kuduPageParams);
        List<Map<String, Object>> result = scanByPredicates(dbName, tableName, kuduPageParams.getParams(), querySize, false);
        if (null != result && result.size() == querySize) {
            kuduPage.setResult(result.subList(0, kuduPageParams.getPageSize()));
            Map<String, Object> last = result.get(result.size() - 1);
            KuduPageParams nkpp = getNextPageParam(kuduPageParams, last);
            kppd.setNextPageParams(nkpp);
        } else {
            if (null != result) {
                kuduPage.setResult(result);
            }
            kppd.setLastPage(true);
        }
        kuduPage.setPageInfo(kppd);
        return kuduPage;
    }

    /**
     * 组装 kudu 分页参数
     *
     * @param kuduPageParams
     * @param last
     * @param <T>
     * @return
     * @throws Exception
     */
    private <T> KuduPageParams getNextPageParam(KuduPageParams kuduPageParams, T last) throws Exception {
        KuduPageParams nkpp = new KuduPageParams();
        nkpp.setPageSize(kuduPageParams.getPageSize());
        nkpp.setParams(new HashMap<>());
        Map lastMap;
        if (last instanceof Map) {
            lastMap = (Map) last;
        } else {
            lastMap = BeanHelper.toMap(last);
        }
        kuduPageParams.getParams().forEach((key, value) -> {
            Object obj = lastMap.get(key);
            if (value.getClass().isArray()) {
                Object objEnd = Array.get(value, 1);
                nkpp.getParams().put(key, new Object[]{obj, objEnd});
            } else {
                nkpp.getParams().put(key, obj);
            }
        });
        return nkpp;
    }


    /**
     * 查询条件转为map
     **/
    private Map<String, Object> predictToMap(List<Predicates> predicateList) {
        Map<String, Object> params = new HashMap<>();
        for (Predicates predicate : predicateList) {
            if (predicate.getValue() != null) {
                params.put(predicate.getKey().toLowerCase(), predicate.getValue());
            } else {
                params.put(predicate.getKey().toLowerCase(), new Object[]{predicate.getLowerBound(), predicate.getUpperBound()});
            }
        }
        return params;
    }

    public List<Map<String, Object>> scan(String dbName, String tableName, Map<String, Object> startParam, Map<String, Object> endParam) {
        return scan(dbName, tableName, startParam, endParam, Constants.MAX_LIMIT, false);
    }

    public List<Map<String, Object>> scan(String dbName, String tableName, Map<String, Object> startParam, Map<String, Object> endParam, int limit) {
        return scan(dbName, tableName, startParam, endParam, limit, false);
    }

    /**
     * 开始条件和结束条件，组合进行查询数据，返回Map
     */
    public List<Map<String, Object>> scan(String dbName, String tableName, Map<String, Object> startParam, Map<String, Object> endParam, int limit, boolean ordered) {
        return KuduOptHelper.scanWithStartEndParam(getTableName(dbName, tableName), mapKeyToLowerCase(startParam), mapKeyToLowerCase(endParam), limit, ordered);
    }

    public <T> List<T> scan(String dbName, String tableName, Map<String, Object> startParam, Map<String, Object> endParam, Class<T> clazz) throws Exception {
        return scan(dbName, tableName, startParam, endParam, clazz, Constants.MAX_LIMIT, false);
    }

    public <T> List<T> scan(String dbName, String tableName, Map<String, Object> startParam, Map<String, Object> endParam, boolean ordered, Class<T> clazz) throws Exception {
        return scan(dbName, tableName, startParam, endParam, clazz, Constants.MAX_LIMIT, ordered);
    }

    public <T> List<T> scan(String dbName, String tableName, Map<String, Object> startParam, Map<String, Object> endParam, int limit, Class<T> clazz) throws Exception {
        return scan(dbName, tableName, startParam, endParam, clazz, limit, false);
    }

    public <T> List<T> scan(String dbName, String tableName, Map<String, Object> startParam, Map<String, Object> endParam, int limit, boolean ordered, Class<T> clazz) throws Exception {
        return scan(dbName, tableName, startParam, endParam, clazz, limit, ordered);
    }

    /**
     * 开始条件和结束条件，组合进行查询数据，返回对象类型
     */
    public <T> List<T> scan(String dbName, String tableName, Map<String, Object> startParam, Map<String, Object> endParam, Class<T> clazz, int limit, boolean ordered) throws Exception {
        List<Map<String, Object>> dataList = KuduOptHelper.scanWithStartEndParam(getTableName(dbName, tableName), mapKeyToLowerCase(startParam), mapKeyToLowerCase(endParam), limit, ordered);
        return extractMapNodeToBean(dataList, clazz);
    }

    /**
     * sql修改表中数据
     *
     * @param dmlSql insert、upsert、update、delete
     * @return DmlResult 数据操纵结果，isSuccess方法获取操作是否成功。
     */
    public DmlResult dmBySql(String dmlSql) throws Exception {
        return excuteDML(dmlSql, false);
    }

    /**
     * sql修改表中数据（同步）
     *
     * @param dmlSql insert、upsert、update、delete
     * @return DmlResult 数据操纵结果，isSuccess方法获取操作是否成功。
     */
    public DmlResult dmBySqlSync(String dmlSql) throws Exception {
        return excuteDML(dmlSql, true);
    }

    /**
     * 通过sql查询,返回bean
     *
     * @param sql select
     * @return DmlResult 数据操纵结果，isSuccess方法获取操作是否成功。
     */
    public <T> List<T> queryBySql(Class<T> clazz, String sql) throws Exception {
        return excuteSqlToBeanList(clazz, sql, false);
    }

    /**
     * 通过sql查询，返回map
     *
     * @param sql select
     * @return DmlResult 数据操纵结果，isSuccess方法获取操作是否成功。
     * @throws Exception
     */
    public List<Map<String, Object>> queryBySql(String sql) throws Exception {
        return excuteSqlToMapList(sql, false);
    }

    /**
     * 通过sql查询（同步）,返回bean
     *
     * @param sql select
     * @return DmlResult 数据操纵结果，isSuccess方法获取操作是否成功。
     */
    public <T> List<T> queryBySqlSync(Class<T> clazz, String sql) throws Exception {
        return excuteSqlToBeanList(clazz, sql, true);
    }

    /**
     * 通过sql查询（同步），返回map
     *
     * @param sql select
     * @return DmlResult 数据操纵结果，isSuccess方法获取操作是否成功。
     */
    public List<Map<String, Object>> queryBySqlSync(String sql) throws Exception {
        return excuteSqlToMapList(sql, true);
    }

    /**
     * 通过sql获取一个对象（异步）,返回bean
     *
     * @param sql select
     * @return DmlResult 数据操纵结果，isSuccess方法获取操作是否成功。
     */
    public <T> T getRowBySql(Class<T> clazz, String sql) throws Exception {
        return excuteSqlToBeanList(clazz, sql, false).get(0);
    }

    /**
     * 通过sql获取一个对象（同步），返回map
     *
     * @param sql select
     * @return DmlResult 数据操纵结果，isSuccess方法获取操作是否成功。
     */
    public Map<String, Object> getRowBySql(String sql) throws Exception {
        return excuteSqlToMapList(sql, true).get(0);
    }

    /**
     * 通过sql获取一个对象（同步）,返回bean
     *
     * @param sql select
     * @return DmlResult 数据操纵结果，isSuccess方法获取操作是否成功。
     */
    public <T> T getRowBySqlSync(Class<T> clazz, String sql) throws Exception {
        return excuteSqlToBeanList(clazz, sql, true).get(0);
    }

    /**
     * 通过sql获取一个对象（同步），返回map
     *
     * @param sql select
     * @return DmlResult 数据操纵结果，isSuccess方法获取操作是否成功。
     */
    public Map<String, Object> getRowBySqlSync(String sql) throws Exception {
        return excuteSqlToMapList(sql, true).get(0);
    }

    private DmlResult excuteDML(String dmlSql, boolean sync) throws Exception {
        int rowsAffected = ImpalaOptHelper.getInstance().impalaOpt(Constants.DEFAULT, dmlSql);
        DmlResult dmlResult = new DmlResult();
        dmlResult.setRowsAffected(rowsAffected);
        return dmlResult;
    }

    private <T> List<T> excuteSqlToBeanList(Class<T> clazz, String sql, boolean sync) throws Exception {
        List<Map<String, Object>> dataList = ImpalaOptHelper.getInstance().impalaQuery(Constants.DEFAULT, sql);
        return extractMapNodeToBean(dataList, clazz);
    }

    private List<Map<String, Object>> excuteSqlToMapList(String sql, boolean sync) throws Exception {
        return ImpalaOptHelper.getInstance().impalaQuery(Constants.DEFAULT, sql);
    }

    private <T> List<T> extractMapNodeToBean(List<Map<String, Object>> dataList, Class<T> clazz) throws Exception {
        //组装返回
        List<T> retList = new ArrayList<>();
        if (dataList.size() != 0) {
            for (Map<String, Object> map : dataList) {
                retList.add(BeanHelper.toObject(map, clazz));
            }
        }
        return retList;
    }

    @Override
    public void close() throws IOException {
        LOG.warn("KuduDataTemplate.close()");
        ImpalaOptHelper.getInstance().close();
        KuduOptHelper.close();
    }

    public static Map<String, Object> mapKeyToLowerCase(Map<String, Object> map) {
        Map<String, Object> retMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            retMap.put(entry.getKey().toLowerCase(), entry.getValue());
        }
        return retMap;
    }
}