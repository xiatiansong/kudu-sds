package org.bg.kudu.model;

import java.util.List;

/**
 * kudu分页查询封装
 * @author: xiatiansong
 * @date: 2019-06-12 20:05
 */
public class KuduPage<T> {

    private List<T> result;

    private KuduPageParamsDto pageInfo;

    public List<T> getResult() {
        return result;
    }

    public void setResult(List<T> result) {
        this.result = result;
    }

    public KuduPageParamsDto getPageInfo() {
        return pageInfo;
    }

    public void setPageInfo(KuduPageParamsDto pageInfo) {
        this.pageInfo = pageInfo;
    }
}
