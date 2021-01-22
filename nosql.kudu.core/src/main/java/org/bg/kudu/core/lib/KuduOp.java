package org.bg.kudu.core.lib;

/**
 * 批量操作的请求类型
 * @author xiatiansong
 *
 */
public enum KuduOp {

	INSERT(1), UPDATE(2), UPSERT(3), DELETE(4);

	private final int value;

	private KuduOp(int value) {
		this.value = value;
	}

	/**
	 * Get the integer value of this enum value, as defined in the Thrift IDL.
	 */
	public int getValue() {
		return value;
	}

	/**
	 * Find a the enum type by its integer value, as defined in the Thrift IDL.
	 * @return null if the value is not found.
	 */
	public static KuduOp findByValue(int value) {
		switch (value) {
		case 1:
			return INSERT;
		case 2:
			return UPDATE;
		case 3:
			return UPSERT;
		case 4:
			return DELETE;
		default:
			return null;
		}
	}
}
