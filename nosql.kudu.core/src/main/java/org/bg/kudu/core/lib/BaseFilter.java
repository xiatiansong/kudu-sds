package org.bg.kudu.core.lib;

import org.apache.kudu.client.KuduPredicate;

/**
 * Class defining a tuple of the type <ColumnName, Operator, value|binary>
 * This is the class that will be used when filtering data inside a kudu table.
 * @author cespedjo
 */
public class BaseFilter {
    
    protected String columnName;
    
    protected KuduPredicate.ComparisonOp operator;
    
    protected Object value;
    
    protected byte[] bValue;
    
    public BaseFilter(){
    	
    }
    
    public BaseFilter(String columnName, Object value, KuduPredicate.ComparisonOp operator) {
		super();
		this.columnName = columnName;
		this.value = value;
		this.operator = operator;
	}
    
    public BaseFilter(String columnName, byte[] bValue, KuduPredicate.ComparisonOp operator) {
		super();
		this.columnName = columnName;
		this.bValue = bValue;
		this.operator = operator;
	}

	public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public KuduPredicate.ComparisonOp getOperator() {
        return operator;
    }

    public void setOperator(KuduPredicate.ComparisonOp operator) {
        this.operator = operator;
    }

    public Object getValue() {
        return this.value;
    }
    
    public byte[] getBinaryValue() {
        return this.bValue;
    }

    public void setValue(Object value) {
        this.value = value;
    }
    
    public void setValue(byte[] value) {
        this.bValue = value;
    }
}
