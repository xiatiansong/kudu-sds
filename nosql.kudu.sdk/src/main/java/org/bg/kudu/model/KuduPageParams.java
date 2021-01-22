package org.bg.kudu.model;

import java.util.Map;

/**
 * kudu分页查询参数
 * @author: xiatiansong
 * @date: 2019-06-13 11:19
 */
public class KuduPageParams {

    private Map<String, Object> params;

    private int pageSize;

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
