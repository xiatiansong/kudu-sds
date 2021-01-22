package org.bg.kudu.core;

import com.google.common.collect.Lists;
import org.bg.kudu.core.lib.BaseFilter;
import org.bg.kudu.core.lib.KuduDataUtil;
import org.bg.kudu.core.lib.KuduOp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduPredicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * kudu增删改查以及scan
 *
 * @author xiatiansong
 */
public class KuduOptHelper {

    private static final Log LOG = LogFactory.getLog(KuduOptHelper.class);

    /**
     * 根据主键获取单条记录
     *
     * @param tableName
     * @param data
     * @return
     */
    public static Map<String, Object> get(String tableName, Map<String, Object> data) {
        List<ColumnSchema> keyList = KuduDataUtil.getPrimaryKeyColumns(KuduHelper.getInstance().getTableSchema(tableName));
        List<BaseFilter> filters = new ArrayList<BaseFilter>();
        for (int i = 0; i < keyList.size(); i++) {
            BaseFilter filter = new BaseFilter();
            filter.setColumnName(keyList.get(i).getName());
            filter.setOperator(KuduPredicate.ComparisonOp.EQUAL);
            filter.setValue(data.get(keyList.get(i).getName()));
            filters.add(filter);
        }
        List<ColumnSchema> columnList = KuduDataUtil.getColumns(KuduHelper.getInstance().getTableSchema(tableName));
        return KuduHelper.getInstance().getByFilter(tableName, filters, columnList);
    }

    /**
     * 批量Get操作
     *
     * @param tableName
     * @param data
     * @return
     */
    public static List<Map<String, Object>> gets(String tableName, List<Map<String, Object>> data) {
        List<Map<String, Object>> columnDatas = Lists.newArrayList();
        List<ColumnSchema> keyList = KuduDataUtil.getPrimaryKeyColumns(KuduHelper.getInstance().getTableSchema(tableName));
        List<ColumnSchema> columnList = KuduDataUtil.getColumns(KuduHelper.getInstance().getTableSchema(tableName));
        for (Map<String, Object> jnData : data) {
            List<BaseFilter> filters = new ArrayList<BaseFilter>();
            for (int i = 0; i < keyList.size(); i++) {
                BaseFilter filter = new BaseFilter();
                filter.setColumnName(keyList.get(i).getName());
                filter.setOperator(KuduPredicate.ComparisonOp.EQUAL);
                filter.setValue(jnData.get(keyList.get(i).getName()));
                filters.add(filter);
            }
            Map<String, Object> dataMap = KuduHelper.getInstance().getByFilter(tableName, filters, columnList);
            if (dataMap == null) {
                continue;
            }
            columnDatas.add(dataMap);
        }
        return columnDatas;
    }

    public static void cudOperate(String tableName, KuduOp op, Map<String, Object> data) {
        KuduHelper.getInstance().cudOperate(tableName, op, data);
    }

    /**
     * 操作数据
     *
     * @param tableName
     * @param dataList
     * @return
     */
    @SuppressWarnings("unchecked")
    public static void cudOperates(String tableName, KuduOp op, List<Map<String, Object>> dataList) {
        if (dataList.size() == 1) {
            KuduHelper.getInstance().cudOperate(tableName, op, dataList.get(0));
        } else if (dataList.size() > 1) {
            KuduHelper.getInstance().batchCudOperate(tableName, op, dataList);
        }
    }

    /**
     * 以开始参数和结束参数扫描记录
     *
     * @param tableName
     * @param startParam
     * @param endParam
     * @param limit
     * @param ordered
     * @return
     */
    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> scanWithStartEndParam(String tableName, Map<String, Object> startParam, Map<String, Object> endParam, int limit, boolean ordered) {
        //获取结果
        List<ColumnSchema> columnList = KuduDataUtil.getColumns(KuduHelper.getInstance().getTableSchema(tableName));
        return KuduHelper.getInstance().scanDataByStartEndParam(tableName, startParam, endParam, limit, ordered, columnList);
    }

    /**
     * 通过各种范围条件来扫描记录
     *
     * @param tableName
     * @param predicates
     * @param limit
     * @param ordered
     * @return
     */
    public static List<Map<String, Object>> scanPredicates(String tableName, Map<String, Object> predicates, int limit, boolean ordered) {
        //设置filter
        List<BaseFilter> filters = new ArrayList<BaseFilter>();
        for (Map.Entry<String, Object> entry : predicates.entrySet()) {
            if (entry.getValue().getClass().isArray()) {
                Object[] array = (Object[]) entry.getValue();
                if (array[0] != null) {
                    //数组时有2个值，即 第一个<= value < 第二个
                    BaseFilter startFilter = new BaseFilter();
                    startFilter.setColumnName(entry.getKey());
                    startFilter.setOperator(KuduPredicate.ComparisonOp.GREATER_EQUAL);
                    startFilter.setValue(array[0]);
                    filters.add(startFilter);
                }
                if (array[1] != null) {
                    //第二个filter
                    BaseFilter endFilter = new BaseFilter();
                    endFilter.setColumnName(entry.getKey());
                    endFilter.setOperator(KuduPredicate.ComparisonOp.LESS_EQUAL);
                    endFilter.setValue(array[1]);
                    filters.add(endFilter);
                }
            } else {
                BaseFilter filter = new BaseFilter();
                filter.setColumnName(entry.getKey());
                filter.setOperator(KuduPredicate.ComparisonOp.EQUAL);
                filter.setValue(entry.getValue());
                filters.add(filter);
            }
        }
        //组装结果
        List<ColumnSchema> columnList = KuduDataUtil.getColumns(KuduHelper.getInstance().getTableSchema(tableName));
        return KuduHelper.getInstance().scanDataByFilter(tableName, filters, limit, ordered, columnList);
    }

    public static void close() throws IOException {
        LOG.warn("KuduOptHelper.close()");
        KuduHelper.getInstance().close();
    }
}