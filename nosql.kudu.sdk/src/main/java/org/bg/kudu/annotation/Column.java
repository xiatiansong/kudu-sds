package org.bg.kudu.annotation;

import org.bg.kudu.annotation.codec.Defaults;
import org.bg.kudu.annotation.codec.TypeCodec;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {

    String name() default "";

    DataType type() default DataType.DEFAULT;

    boolean primaryKey() default false;

    boolean nullable() default true;

    Class<? extends TypeCodec<?>> codec() default Defaults.NoCodec.class;
}
