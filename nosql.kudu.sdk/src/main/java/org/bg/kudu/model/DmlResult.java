package org.bg.kudu.model;

public class DmlResult {

    private Integer rowsAffected;

    private Integer rows;

    public Integer getRowsAffected() {
        return rowsAffected;
    }

    public void setRowsAffected(Integer rowsAffected) {
        this.rowsAffected = rowsAffected;
    }

    public Integer getRows() {
        return rows;
    }

    public void setRows(Integer rows) {
        this.rows = rows;
    }

    public boolean isSuccessed() {
        return rows == -1 && rowsAffected == -1;
    }
}
