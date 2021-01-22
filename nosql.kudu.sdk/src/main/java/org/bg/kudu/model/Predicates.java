package org.bg.kudu.model;

/**
 * 配置查询条件
 * @author xiatiansong
 *
 */
public class Predicates {
	//查询字段
	private String key;
	//设置此值表示使用相等条件，即 key = value
	private Object value;
	//比较条件的左值
	private Object lowerBound;
	//比较条件的右值
	private Object upperBound;

	public Predicates() {
	}

	public Predicates(String key, Object value) {
		super();
		this.key = key;
		this.value = value;
	}

	public Predicates(String key, Object lowerBound, Object upperBound) {
		super();
		this.key = key;
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public Object getLowerBound() {
		return lowerBound;
	}

	public void setLowerBound(Object lowerBound) {
		this.lowerBound = lowerBound;
	}

	public Object getUpperBound() {
		return upperBound;
	}

	public void setUpperBound(Object upperBound) {
		this.upperBound = upperBound;
	}
}