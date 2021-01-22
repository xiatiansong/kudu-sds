package org.bg.kudu.model;

import org.bg.kudu.util.Constants;

import java.util.Map;

/**
 * scan请求参数
 */
public class ScanRequest {

    private Map<String, Object> params;

    private Map<String, Object> startParam;

    private Map<String, Object> endParam;

    private int limit = Constants.MAX_LIMIT;

    private boolean ordered = false;

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public boolean isOrdered() {
        return ordered;
    }

    public void setOrdered(boolean ordered) {
        this.ordered = ordered;
    }

    public Map<String, Object> getStartParam() {
        return startParam;
    }

    public void setStartParam(Map<String, Object> startParam) {
        this.startParam = startParam;
    }

    public Map<String, Object> getEndParam() {
        return endParam;
    }

    public void setEndParam(Map<String, Object> endParam) {
        this.endParam = endParam;
    }
}