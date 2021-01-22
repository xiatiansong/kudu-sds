package org.bg.kudu.annotation;

import java.math.BigDecimal;

public enum DataType {
    //
    DEFAULT(null),
    TINYINT(Byte.class),
    SMALLINT(Short.class),
    INT(Integer.class),
    BIGINT(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    BOOLEAN(Boolean.class),
    STRING(String.class),
    DECIMAL(BigDecimal.class);

    private Class clazz;

    DataType(Class clazz) {
        this.clazz = clazz;
    }

    public Class getClazz() {
        return clazz;
    }
}
