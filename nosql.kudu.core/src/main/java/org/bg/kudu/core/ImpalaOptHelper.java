package org.bg.kudu.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.bg.kudu.util.Constants;
import org.bg.kudu.util.LogUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Impala操作服务类
 *
 * @author xiatiansong
 */
public class ImpalaOptHelper {

    private static final Log LOG = LogFactory.getLog(ImpalaOptHelper.class);

    private static ImpalaOptHelper INSTANCE;

    private ImpalaDbConnHelper impalaDbConnHelper = null;
    private ImpalaConnHelper impalaConnHelper = null;

    private ImpalaOptHelper(String url, String driverName, String username, String password) {
        try {
            // 初始化
            LOG.warn("impala server:" + url + ":" + driverName);
            impalaDbConnHelper = ImpalaDbConnHelper.getInstance(url, username == null ? "" : username, password == null ? "" : password, driverName);
            impalaConnHelper = ImpalaConnHelper.getInstance(url, username == null ? "" : username, password == null ? "" : password, driverName, "default");
        } catch (Exception e) {
            LOG.error(LogUtil.getStackTrace(e), e);
        }
    }


    public static ImpalaOptHelper getInstance() {
        assert (INSTANCE != null);
        return INSTANCE;
    }

    /**
     * 根据kudu master 集群ip初始化客户端
     *
     * @param url
     * @param driverName
     * @param username
     * @param password
     * @return
     */
    public synchronized static ImpalaOptHelper getInstance(String url, String driverName, String username, String password) {
        if (INSTANCE == null) {
            INSTANCE = new ImpalaOptHelper(url, driverName, username, password);
        }
        return INSTANCE;
    }

    /**
     * <pre>
     *  impala 查询
     * </pre>
     *
     * @param database
     * @param sql
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> impalaQuery(String database, String sql) throws SQLException {
        ResultSet rs = null;
        try {
            if(StringUtils.isEmpty(database)){
                database = Constants.DEFAULT;
            }
            rs = impalaDbConnHelper.getImpalaHelper(database).executeQuery(sql);
            // 查询
            return processQuery(rs);
        } catch (SQLException e) {
            LOG.error(LogUtil.getAllStackTrace(e), e);
            rs = impalaDbConnHelper.getNewImpalaHelper(database).executeQuery(sql);
            // 查询
            return processQuery(rs);
        } finally {
            if (rs != null) {
                impalaConnHelper.closeResultSet(rs);
            }
        }
    }

    /**
     * <pre>
     *     impala crud 操作
     * </pre>
     *
     * @param database
     * @param sql
     * @return
     * @throws SQLException
     */
    public int impalaOpt(String database, String sql) throws SQLException {
        Matcher match = Constants.ImpalaConstants.QUERY_CMD_PATTERN.matcher(sql);
        if (!match.matches()) {
            throw new SQLException("illegal sql statement");
        }
        if(StringUtils.isEmpty(database)){
            database = Constants.DEFAULT;
        }
        String cmd = match.group(1).toLowerCase();
        if ("upsert".equals(cmd) || "update".equals(cmd) || "delete".equals(cmd) || "create".equals(cmd) || "drop".equals(cmd) || "insert".equals(cmd)) {
            return impalaDbConnHelper.getImpalaHelper(database).execute(sql);
        }
        return 0;
    }

    /**
     * impala query 查询
     *
     * @param rs
     * @throws SQLException
     */
    private List<Map<String, Object>> processQuery(ResultSet rs) throws SQLException {
        List<Map<String, Object>> retList = Lists.newArrayList();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            Map<String, Object> map = Maps.newHashMap();
            for (int i = 0; i < columnCount; i++) {
                Object e = rs.getObject(i + 1);
                map.put(metaData.getColumnName(i + 1), e);
            }
            retList.add(map);
        }
        return retList;
    }

    public void close() throws IOException {
        impalaDbConnHelper.close();
    }
}