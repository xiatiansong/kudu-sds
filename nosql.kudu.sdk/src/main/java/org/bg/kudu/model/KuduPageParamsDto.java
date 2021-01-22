package org.bg.kudu.model;

/**
 * kudu分页信息返回封装
 * @author: xiatiansong
 * @date: 2019-06-13 11:29
 */
public class KuduPageParamsDto {

    private KuduPageParams currentPageParams;

    private KuduPageParams nextPageParams;

    private boolean isLastPage = false;

    public KuduPageParams getNextPageParams() {
        return nextPageParams;
    }

    public void setNextPageParams(KuduPageParams nextPageParams) {
        this.nextPageParams = nextPageParams;
    }

    public KuduPageParams getCurrentPageParams() {
        return currentPageParams;
    }

    public void setCurrentPageParams(KuduPageParams currentPageParams) {
        this.currentPageParams = currentPageParams;
    }

    public boolean isLastPage() {
        return isLastPage;
    }

    public void setLastPage(boolean lastPage) {
        isLastPage = lastPage;
    }
}
