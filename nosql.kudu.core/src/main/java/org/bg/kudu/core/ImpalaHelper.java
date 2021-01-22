package org.bg.kudu.core;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.bg.kudu.util.LogUtil;

/**
 * Impala常用操作工具类
 *
 * @author xiatiansong
 */
public class ImpalaHelper implements Closeable {

    private static final Log LOG = LogFactory.getLog(ImpalaHelper.class);

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    private static ImpalaHelper INSTANCE;
    private String url;
    private String database;
    private String username;
    private String password;
    private Connection connection;

    private ImpalaHelper that;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    if (INSTANCE != null) {
                        INSTANCE.close();
                    }
                } catch (IOException e) {
                    LOG.error("error close connection");
                }
            }
        });
    }

    /**
     * 启动任务
     */
    private void startTimerTask() {
        that = this;
        Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    ResultSet rs = executeQuery("select 1");
                    rs.getStatement().close();
                    rs.close();
                    LOG.warn("keepalive to impala！");
                } catch (Exception ex) {
                    that.newConn();
                    LOG.error(LogUtil.getAllStackTrace(ex));
                }
            }
        };
        scheduler.scheduleAtFixedRate(task, 5, 20, TimeUnit.MINUTES);
    }

    /**
     * 获取impala操作示例
     *
     * @return
     * @throws IOException
     */
    public static ImpalaHelper getInstance() throws IOException {
        assert (INSTANCE != null);
        return INSTANCE;
    }

    /**
     * 初始化impala操作
     *
     * @return
     * @throws IOException
     */
    public synchronized static ImpalaHelper getInstance(String url, String username, String password, String driverName, String database) {
        if (INSTANCE == null) {
            INSTANCE = new ImpalaHelper(url, username, password, database, driverName);
        }
        return INSTANCE;
    }

    /**
     * 初始化impala操作
     *
     * @return
     * @throws IOException
     */
    public synchronized static ImpalaHelper getNewInstance(String url, String username, String password, String driverName, String database) {
        return new ImpalaHelper(url, database, username, password, driverName);
    }

    /**
     * Stops the singleton instance and cleans up the internal reference.
     */
    public synchronized static void stop() {
        if (INSTANCE != null) {
            INSTANCE = null;
        }
    }

    /**
     * Internal constructor, called by the <code>getInstance()</code> methods.
     *
     * @param url
     * @throws IOException When creating the remote HBase connection fails.
     */
    private ImpalaHelper(String url, String database, String username, String password, String driverName) {
        try {
            Class.forName(driverName);
            this.url = url;
            this.database = database;
            this.username = username;
            this.password = password;
            if (database != null) {
                this.url = url.replace("default", database);
            }
            if (StringUtils.isEmpty(this.username) && StringUtils.isEmpty(this.password)) {
                this.connection = DriverManager.getConnection(this.url);
            } else {
                this.connection = DriverManager.getConnection(this.url, this.username, this.password);
            }
            this.startTimerTask();
        } catch (ClassNotFoundException e) {
            LOG.error(LogUtil.getStackTrace(e), e);
            throw new RuntimeException(e);
        } catch (SQLException e) {
            LOG.error(LogUtil.getStackTrace(e), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (null != connection) {
            try {
                connection.close();
                connection = null;
                scheduler.shutdownNow();
            } catch (SQLException e) {
                LOG.error(LogUtil.getStackTrace(e), e);
                throw new RuntimeException(e);
            }
        }
    }

    private void checkConn() {
        try {
            if (null == connection || connection.isClosed()) {
                if (StringUtils.isEmpty(this.username) && StringUtils.isEmpty(this.password)) {
                    this.connection = DriverManager.getConnection(this.url);
                } else {
                    this.connection = DriverManager.getConnection(this.url, this.username, this.password);
                }
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void newConn() {
        try {
            if (null != connection) {
                connection.close();
                connection = null;
            }
            if (StringUtils.isEmpty(this.username) && StringUtils.isEmpty(this.password)) {
                this.connection = DriverManager.getConnection(this.url);
            } else {
                this.connection = DriverManager.getConnection(this.url, this.username, this.password);
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public Connection getConn() {
        checkConn();
        return connection;
    }

    /**
     * 执行增、删、改/创建、删除表
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public int execute(String sql) throws SQLException {
        LOG.info(sql);
        Statement stmt = null;
        int result = 0;
        try {
            stmt = getConn().createStatement();
            result = stmt.executeUpdate(sql);
        } catch (SQLException e) {
            newConn();
            LOG.warn(LogUtil.getStackTrace(e));
            throw e;
        } finally {
            stmt.close();
        }
        return result;
    }

    /**
     * 增删改、创建、删除表
     *
     * @param sql
     * @param parameters
     * @return
     * @throws SQLException
     */
    public int execute(String sql, List<Object> parameters) throws SQLException {
        LOG.info(sql);
        LOG.info(parameters.toString());

        PreparedStatement stmt = null;
        int result = 0;
        try {
            stmt = getConn().prepareStatement(sql);
            int index = 1;
            for (Object parameter : parameters) {
                stmt.setObject(index, parameter);
                index++;
            }
            result = stmt.executeUpdate(sql);
        } catch (SQLException e) {
            newConn();
            LOG.warn(LogUtil.getStackTrace(e));
            throw e;
        } finally {
            stmt.close();
        }
        return result;
    }

    /**
     * 查询数据
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        LOG.info(sql);
        ResultSet rs = null;
        try {
            Statement stmt = getConn().createStatement();
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            newConn();
            LOG.warn(LogUtil.getStackTrace(e));
            throw e;
        }
        return rs;
    }

    /**
     * 查询数据
     *
     * @param sql
     * @param parameters
     * @return
     * @throws SQLException
     */
    public ResultSet executeQuery(String sql, List<Object> parameters) throws SQLException {
        LOG.info(sql);
        LOG.info(parameters.toString());

        ResultSet rs = null;
        try {
            PreparedStatement stmt = getConn().prepareStatement(sql);
            int index = 1;
            for (Object parameter : parameters) {
                stmt.setObject(index, parameter);
                index++;
            }
            rs = stmt.executeQuery();
        } catch (SQLException e) {
            newConn();
            LOG.warn(LogUtil.getStackTrace(e));
            throw e;
        }
        return rs;
    }

    public void closeResultSet(ResultSet rs) throws SQLException {
        if (null != rs) {
            Statement stmt = rs.getStatement();
            rs.close();
            stmt.close();
        }
    }
}