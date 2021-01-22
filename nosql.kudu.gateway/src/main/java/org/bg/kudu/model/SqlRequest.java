package org.bg.kudu.model;

import org.bg.kudu.util.Constants;

/**
 * sql 请求参数
 */
public class SqlRequest {

    private String database = Constants.DEFAULT;

    private String sql = "";

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}