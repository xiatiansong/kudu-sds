package org.bg.kudu.core.lib;

import org.bg.kudu.util.Constants;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * kudu操作工具类
 *
 * @author xiatiansong
 */
public class KuduDataUtil {

    private static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd hh:mm:ss.SSS";

    /**
     * 设置数据
     *
     * @param row
     * @param columnName
     * @param schema
     * @param value
     * @throws Exception
     */
    public static void addColumnValue(PartialRow row, String columnName, Schema schema, Object value) throws Exception {
        ColumnSchema cs = schema.getColumn(columnName);
        if (cs == null) {
            throw new Exception("Invalid column name: " + columnName);
        }
        //Default value for null
        if (value == null) {
            row.addObject(columnName, null);
        } else if (cs.getType() == Type.STRING) {
            row.addString(columnName, String.valueOf(value));
        } else if (cs.getType() == Type.BINARY) {
            row.addBinary(columnName, (byte[]) value);
        } else if (cs.getType() == Type.BOOL) {
            row.addBoolean(columnName, Boolean.parseBoolean(String.valueOf(value)));
        } else if (cs.getType() == Type.DOUBLE) {
            row.addDouble(columnName, Double.parseDouble(String.valueOf(value)));
        } else if (cs.getType() == Type.FLOAT) {
            row.addFloat(columnName, Float.parseFloat(String.valueOf(value)));
        } else if (cs.getType() == Type.DECIMAL) {
            row.addDecimal(columnName, new BigDecimal(String.valueOf(value)));
        } else if (cs.getType() == Type.INT8) {
            row.addByte(columnName, Byte.parseByte(String.valueOf(value)));
        } else if (cs.getType() == Type.INT16) {
            row.addShort(columnName, Short.parseShort(String.valueOf(value)));
        } else if (cs.getType() == Type.INT32) {
            row.addInt(columnName, Integer.parseInt(String.valueOf(value)));
        } else if (cs.getType() == Type.INT64) {
            row.addLong(columnName, Long.parseLong(String.valueOf(value)));
        } else if (cs.getType() == Type.UNIXTIME_MICROS) {
            //Convert to long millis
            row.addLong(columnName, new SimpleDateFormat(DEFAULT_TIMESTAMP_FORMAT).parse(String.valueOf(value)).getTime());
        } else {
            throw new Exception("Invalid format: " + cs.getType().getName() + " when inserting value " + String.valueOf(value) + " in column:" + columnName);
        }
    }

    /**
     * 根据filter生成查询条件
     *
     * @param filter
     * @param kuduTable
     * @return
     */
    public static KuduPredicate getPredicate(BaseFilter filter, KuduTable kuduTable) {
        ColumnSchema cs = kuduTable.getSchema().getColumn(filter.getColumnName());
        KuduPredicate p = null;
        if (cs.getType().getDataType(null) == Common.DataType.DOUBLE) {
            p = KuduPredicate.newComparisonPredicate(cs, filter.getOperator(), (double) filter.getValue());
        } else if (cs.getType().getDataType(null) == Common.DataType.BOOL) {
            p = KuduPredicate.newComparisonPredicate(cs, filter.getOperator(), (boolean) filter.getValue());
        } else if (cs.getType().getDataType(null) == Common.DataType.INT8) {
            p = KuduPredicate.newComparisonPredicate(cs, filter.getOperator(), (byte) filter.getValue());
        } else if (cs.getType().getDataType(null) == Common.DataType.INT16) {
            p = KuduPredicate.newComparisonPredicate(cs, filter.getOperator(), (short) filter.getValue());
        } else if (cs.getType().getDataType(null) == Common.DataType.INT32) {
            p = KuduPredicate.newComparisonPredicate(cs, filter.getOperator(), (int) filter.getValue());
        } else if (cs.getType().getDataType(null) == Common.DataType.INT64 || cs.getType().getDataType(null) == Common.DataType.UNIXTIME_MICROS) {
            p = KuduPredicate.newComparisonPredicate(cs, filter.getOperator(), (long) filter.getValue());
        } else if (cs.getType().getDataType(null) == Common.DataType.STRING) {
            p = KuduPredicate.newComparisonPredicate(cs, filter.getOperator(), filter.getValue().toString());
        } else if (cs.getType().getDataType(null) == Common.DataType.BINARY) {
            p = KuduPredicate.newComparisonPredicate(cs, filter.getOperator(), filter.getBinaryValue());
        } else if (cs.getType().getDataType(null) == Common.DataType.FLOAT) {
            p = KuduPredicate.newComparisonPredicate(cs, filter.getOperator(), (float) filter.getValue());
        } else {
            p = KuduPredicate.newComparisonPredicate(cs, filter.getOperator(), filter.getValue().toString());
        }
        return p;
    }

    /**
     * 设置主见范围查询参数
     *
     * @param param
     * @param kuduTable
     * @return
     * @throws Exception
     */
    public static PartialRow getPartialRow(Map<String, Object> param, KuduTable kuduTable, boolean start) throws Exception {
        PartialRow row = kuduTable.getSchema().newPartialRow();
        List<ColumnSchema> cols = getPrimaryKeyColumns(kuduTable.getSchema());
        if (!param.isEmpty()) {
            for (ColumnSchema cs : cols) {
                String columnName = cs.getName();
                Object value = param.get(columnName);
                if (value != null) {
                    if (cs.getType() == Type.STRING) {
                        row.addString(columnName, String.valueOf(value));
                    } else if (cs.getType() == Type.BINARY) {
                        row.addBinary(columnName, (byte[]) value);
                    } else if (cs.getType() == Type.BOOL) {
                        row.addBoolean(columnName, Boolean.parseBoolean(String.valueOf(value)));
                    } else if (cs.getType() == Type.DOUBLE) {
                        row.addDouble(columnName, Double.parseDouble(String.valueOf(value)));
                    } else if (cs.getType() == Type.FLOAT) {
                        row.addFloat(columnName, Float.parseFloat(String.valueOf(value)));
                    } else if (cs.getType() == Type.DECIMAL) {
                        row.addDecimal(columnName, new BigDecimal(String.valueOf(value)));
                    } else if (cs.getType() == Type.INT8) {
                        row.addByte(columnName, Byte.parseByte(String.valueOf(value)));
                    } else if (cs.getType() == Type.INT16) {
                        row.addShort(columnName, Short.parseShort(String.valueOf(value)));
                    } else if (cs.getType() == Type.INT32) {
                        row.addInt(columnName, Integer.parseInt(String.valueOf(value)));
                    } else if (cs.getType() == Type.INT64) {
                        row.addLong(columnName, Long.parseLong(String.valueOf(value)));
                    } else if (cs.getType() == Type.UNIXTIME_MICROS) {
                        //Convert to long millis
                        row.addLong(columnName, new SimpleDateFormat(DEFAULT_TIMESTAMP_FORMAT).parse(String.valueOf(value)).getTime());
                    } else {
                        row.addString(columnName, String.valueOf(value));
                    }
                } else {
                    if (cs.getType() == Type.STRING) {
                        row.addString(columnName, Bytes.getString(start ? Constants.KuduConstants.START_ROW : Constants.KuduConstants.STOP_ROW));
                    } else if (cs.getType() == Type.BINARY) {
                        row.addBinary(columnName, start ? Constants.KuduConstants.START_ROW : Constants.KuduConstants.STOP_ROW);
                    } else if (cs.getType() == Type.BOOL) {
                        row.addBoolean(columnName, start ? false : true);
                    } else if (cs.getType() == Type.DOUBLE) {
                        row.addDouble(columnName, start ? Double.MIN_VALUE : Double.MAX_VALUE);
                    } else if (cs.getType() == Type.FLOAT) {
                        row.addFloat(columnName, start ? Float.MIN_VALUE : Float.MAX_VALUE);
                    } else if (cs.getType() == Type.DECIMAL) {
                        row.addDecimal(columnName, start ? BigDecimal.valueOf(Double.MIN_VALUE) : BigDecimal.valueOf(Double.MAX_VALUE));
                    } else if (cs.getType() == Type.INT8) {
                        row.addByte(columnName, start ? Byte.MIN_VALUE : Byte.MAX_VALUE);
                    } else if (cs.getType() == Type.INT16) {
                        row.addShort(columnName, start ? Short.MIN_VALUE : Short.MAX_VALUE);
                    } else if (cs.getType() == Type.INT32) {
                        row.addInt(columnName, start ? Integer.MIN_VALUE : Integer.MAX_VALUE);
                    } else if (cs.getType() == Type.INT64) {
                        row.addLong(columnName, start ? Long.MIN_VALUE : Long.MAX_VALUE);
                    } else if (cs.getType() == Type.UNIXTIME_MICROS) {
                        //Convert to long millis
                        row.addLong(columnName, start ? Long.MIN_VALUE : Long.MAX_VALUE);
                    } else {
                        row.addString(columnName, String.valueOf(value));
                    }
                }
            }
        }
        return row;
    }

    /**
     * 获取RowResult值
     *
     * @param rs
     * @param cs
     * @return
     * @throws ParseException
     * @throws IOException
     */
    public static void addResultValue(RowResult rs, ColumnSchema cs, Map<String, Object> dataMap) {
        //Default value for null
        String columnName = cs.getName();
        Object value = null;
        if (rs.isNull(columnName)) {
            value = null;
        } else if (cs.getType() == Type.STRING) {
            value = rs.getString(columnName);
        } else if (cs.getType() == Type.BINARY) {
            value = rs.getBinary(columnName).array();
        } else if (cs.getType() == Type.BOOL) {
            value = rs.getBoolean(columnName);
        } else if (cs.getType() == Type.DOUBLE) {
            value = rs.getDouble(columnName);
        } else if (cs.getType() == Type.FLOAT) {
            value = rs.getFloat(columnName);
        } else if (cs.getType() == Type.DECIMAL) {
            value = rs.getDecimal(columnName);
        } else if (cs.getType() == Type.INT8) {
            value = rs.getByte(columnName);
        } else if (cs.getType() == Type.INT16) {
            value = rs.getShort(columnName);
        } else if (cs.getType() == Type.INT32) {
            value = rs.getInt(columnName);
        } else if (cs.getType() == Type.INT64) {
            value = rs.getLong(columnName);
        } else if (cs.getType() == Type.UNIXTIME_MICROS) {
            value = rs.getLong(columnName);
        } else {
            value = rs.getString(columnName);
        }
        dataMap.put(columnName, value);
    }

    /**
     * 获取主键的列名称和列类型
     *
     * @param schema
     * @return
     * @throws Exception
     */
    public static List<ColumnSchema> getPrimaryKeyColumns(Schema schema) {
        List<ColumnSchema> schemaList = new ArrayList<ColumnSchema>();
        for (ColumnSchema colSchema : schema.getColumns()) {
            if (!colSchema.isKey()) {
                continue;
            }
            schemaList.add(colSchema);
        }
        return schemaList;
    }

    /**
     * 获取主键的列名称和列类型
     *
     * @param schema
     * @return
     * @throws Exception
     */
    public static Map<String, ColumnSchema> getPrimaryKeyColumnMap(Schema schema) {
        Map<String, ColumnSchema> typePair = new HashMap<String, ColumnSchema>();
        for (ColumnSchema colSchema : schema.getColumns()) {
            if (!colSchema.isKey()) {
                continue;
            }
            typePair.put(colSchema.getName(), colSchema);
        }
        return typePair;
    }

    /**
     * 获取type
     *
     * @param schema
     * @return
     * @throws Exception
     */
    public static Map<String, ColumnSchema> getColumnsTypePair(Schema schema) throws Exception {
        Map<String, ColumnSchema> typePair = new HashMap<String, ColumnSchema>();
        for (ColumnSchema colSchema : schema.getColumns()) {
            typePair.put(colSchema.getName(), colSchema);
        }
        return typePair;
    }

    public static List<ColumnSchema> getColumnsWithoutPrimaryKey(Schema schema) {
        List<ColumnSchema> schemaList = new ArrayList<ColumnSchema>();
        for (ColumnSchema colSchema : schema.getColumns()) {
            if (colSchema.isKey()) {
                continue;
            }
            schemaList.add(colSchema);
        }
        return schemaList;
    }

    /**
     * 获取列名称和列类型
     *
     * @param schema
     * @return
     * @throws Exception
     */
    public static List<ColumnSchema> getColumns(Schema schema) {
        return schema.getColumns();
    }
}