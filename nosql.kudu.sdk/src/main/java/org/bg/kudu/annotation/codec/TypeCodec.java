package org.bg.kudu.annotation.codec;

import com.google.common.reflect.TypeToken;
import org.bg.kudu.annotation.DataType;
import org.bg.kudu.annotation.InvalidTypeException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public abstract class TypeCodec<T> {

    private static Pattern patternInteger = Pattern.compile("^[-\\+]?[\\d]*$");
    private static Pattern patternFloat = Pattern.compile("^[-\\+]?[.\\d]*$");

    protected final TypeToken<T> javaType;

    protected final DataType cqlType;

    protected TypeCodec(DataType cqlType, Class<T> javaClass) {
        this(cqlType, TypeToken.of(javaClass));
    }

    protected TypeCodec(DataType cqlType, TypeToken<T> javaType) {
        checkNotNull(cqlType, "cqlType cannot be null");
        checkNotNull(javaType, "javaType cannot be null");
        checkArgument(!javaType.isPrimitive(), "Cannot create a codec for a primitive Java type (%s), please use the wrapper type instead", javaType);
        this.cqlType = cqlType;
        this.javaType = javaType;
    }

    public TypeToken<T> getJavaType() {
        return javaType;
    }

    public DataType getKuduType() {
        return cqlType;
    }

    public abstract Object serialize(T value) throws InvalidTypeException;

    public abstract T deserialize(Object object) throws InvalidTypeException;

    public abstract T parse(String value) throws InvalidTypeException;

    public abstract String format(T value) throws InvalidTypeException;

    public boolean accepts(TypeToken<?> javaType) {
        checkNotNull(javaType, "Parameter javaType cannot be null");
        return this.javaType.equals(javaType.wrap());
    }

    public boolean accepts(Class<?> javaType) {
        checkNotNull(javaType, "Parameter javaType cannot be null");
        return accepts(TypeToken.of(javaType));
    }

    public boolean accepts(DataType cqlType) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        return this.cqlType.equals(cqlType);
    }


    public static PrimitiveBooleanCodec kboolean() {
        return BooleanCodec.instance;
    }

    public static PrimitiveByteCodec tinyInt() {
        return TinyIntCodec.instance;
    }

    public static PrimitiveShortCodec smallInt() {
        return SmallIntCodec.instance;
    }

    public static PrimitiveIntCodec kint() {
        return IntCodec.instance;
    }

    public static PrimitiveBigintCodec bigint() {
        return BigintCodec.instance;
    }

    public static PrimitiveFloatCodec kfloat() {
        return FloatCodec.instance;
    }

    public static PrimitiveDoubleCodec kdouble() {
        return DoubleCodec.instance;
    }

    public static PrimitiveStringCodec kstring() {
        return StringCodec.instance;
    }

    public static TypeCodec<BigDecimal> decimal() {
        return DecimalCodec.instance;
    }

    public abstract static class PrimitiveBooleanCodec extends TypeCodec<Boolean> {
        protected PrimitiveBooleanCodec(DataType cqlType) {
            super(cqlType, Boolean.class);
        }

        public abstract Object serializeNoBoxing(boolean var1);

        public abstract boolean deserializeNoBoxing(Object obj);

        public Object serialize(Boolean value) {
            return value == null ? null : this.serializeNoBoxing(value);
        }

        public Boolean deserialize(Object obj) {
            return obj != null ? this.deserializeNoBoxing(obj) : null;
        }
    }

    public abstract static class PrimitiveByteCodec extends TypeCodec<Byte> {
        protected PrimitiveByteCodec(DataType cqlType) {
            super(cqlType, Byte.class);
        }

        public abstract Object serializeNoBoxing(byte var1);

        public abstract byte deserializeNoBoxing(Object obj);

        public Object serialize(Byte value) {
            return value == null ? null : this.serializeNoBoxing(value);
        }

        public Byte deserialize(Object obj) {
            return obj != null ? this.deserializeNoBoxing(obj) : null;
        }
    }

    public abstract static class PrimitiveShortCodec extends TypeCodec<Short> {
        protected PrimitiveShortCodec(DataType cqlType) {
            super(cqlType, Short.class);
        }

        public abstract Object serializeNoBoxing(short var1);

        public abstract short deserializeNoBoxing(Object var1);

        public Object serialize(Short value) {
            return value == null ? null : this.serializeNoBoxing(value);
        }

        public Short deserialize(Object obj) {
            return obj != null ? this.deserializeNoBoxing(obj) : null;
        }
    }

    public abstract static class PrimitiveIntCodec extends TypeCodec<Integer> {
        protected PrimitiveIntCodec(DataType cqlType) {
            super(cqlType, Integer.class);
        }

        public abstract Object serializeNoBoxing(int var1);

        public abstract int deserializeNoBoxing(Object var1);

        public Object serialize(Integer value) {
            return value == null ? null : this.serializeNoBoxing(value);
        }

        public Integer deserialize(Object obj) {
            return obj != null ? this.deserializeNoBoxing(obj) : null;
        }
    }

    public abstract static class PrimitiveBigintCodec extends TypeCodec<Long> {
        protected PrimitiveBigintCodec(DataType cqlType) {
            super(cqlType, Long.class);
        }

        public abstract Object serializeNoBoxing(long var1);

        public abstract long deserializeNoBoxing(Object var1);

        public Object serialize(Long value) {
            return value == null ? null : this.serializeNoBoxing(value);
        }

        public Long deserialize(Object obj) {
            return obj != null ? this.deserializeNoBoxing(obj) : null;
        }
    }

    public abstract static class PrimitiveDoubleCodec extends TypeCodec<Double> {
        protected PrimitiveDoubleCodec(DataType cqlType) {
            super(cqlType, Double.class);
        }

        public abstract Object serializeNoBoxing(double var1);

        public abstract double deserializeNoBoxing(Object obj);

        public Object serialize(Double value) {
            return value == null ? null : this.serializeNoBoxing(value);
        }

        public Double deserialize(Object obj) {
            return obj != null ? this.deserializeNoBoxing(obj) : null;
        }
    }

    public abstract static class PrimitiveStringCodec extends TypeCodec<String> {
        protected PrimitiveStringCodec(DataType cqlType) {
            super(cqlType, String.class);
        }

        public abstract Object serializeNoBoxing(String str);

        public abstract String deserializeNoBoxing(Object obj);

        public Object serialize(String value) {
            return value == null ? null : this.serializeNoBoxing(value);
        }

        public String deserialize(Object obj) {
            return obj != null ? this.deserializeNoBoxing(obj) : null;
        }
    }

    public abstract static class PrimitiveFloatCodec extends TypeCodec<Float> {
        protected PrimitiveFloatCodec(DataType cqlType) {
            super(cqlType, Float.class);
        }

        public abstract Object serializeNoBoxing(float var1);

        public abstract float deserializeNoBoxing(Object var1);

        public Object serialize(Float value) {
            return value == null ? null : this.serializeNoBoxing(value);
        }

        public Float deserialize(Object obj) {
            return obj != null ? this.deserializeNoBoxing(obj) : null;
        }
    }

    private static class DecimalCodec extends TypeCodec<BigDecimal> {
        private static final DecimalCodec instance = new DecimalCodec();

        private DecimalCodec() {
            super(DataType.DECIMAL, BigDecimal.class);
        }

        public BigDecimal parse(String value) {
            try {
                return value != null && !value.isEmpty() && !value.equalsIgnoreCase("NULL") ? new BigDecimal(value) : null;
            } catch (NumberFormatException var3) {
                throw new InvalidTypeException(String.format("Cannot parse decimal value from \"%s\"", value));
            }
        }

        public String format(BigDecimal value) {
            return value == null ? "NULL" : value.toString();
        }

        public Object serialize(BigDecimal value) {
            if (value == null) {
                return null;
            } else {
                return value;
            }
        }

        public BigDecimal deserialize(Object obj) {
            if (obj != null) {
                if (!patternFloat.matcher(obj.toString()).matches() && !patternInteger.matcher(obj.toString()).matches()) {
                    throw new InvalidTypeException("Invalid decimal value, expecting at least 4 bytes but got " + obj.toString());
                } else {
                    if (obj instanceof BigDecimal) {
                        return (BigDecimal) obj;
                    } else if (obj instanceof String) {
                        return new BigDecimal((String) obj);
                    } else if (obj instanceof BigInteger) {
                        return new BigDecimal((BigInteger) obj);
                    } else if (obj instanceof Number) {
                        return new BigDecimal(((Number) obj).doubleValue());
                    } else {
                        throw new InvalidTypeException("Not possible to coerce [" + obj + "] from class " + obj.getClass() + " into a BigDecimal.");
                    }
                }
            } else {
                return null;
            }
        }
    }

    private static class BooleanCodec extends PrimitiveBooleanCodec {
        private static final BooleanCodec instance = new BooleanCodec();

        private BooleanCodec() {
            super(DataType.BOOLEAN);
        }

        public Boolean parse(String value) {
            if (value != null && !value.isEmpty() && !value.equalsIgnoreCase("NULL")) {
                if (value.equalsIgnoreCase(Boolean.FALSE.toString())) {
                    return false;
                } else if (value.equalsIgnoreCase(Boolean.TRUE.toString())) {
                    return true;
                } else {
                    throw new InvalidTypeException(String.format("Cannot parse boolean value from \"%s\"", value));
                }
            } else {
                return null;
            }
        }

        public String format(Boolean value) {
            if (value == null) {
                return "NULL";
            } else {
                return value ? "true" : "false";
            }
        }

        public Object serializeNoBoxing(boolean value) {
            return value ? Boolean.TRUE : Boolean.FALSE;
        }

        public boolean deserializeNoBoxing(Object obj) {
            if (obj != null) {
                if (!"true".equals(obj.toString()) && !"false".equals(obj.toString())) {
                    throw new InvalidTypeException("Invalid boolean value, expecting true/false but got " + obj.toString());
                } else {
                    return Boolean.valueOf(obj.toString());
                }
            } else {
                return false;
            }
        }
    }

    private static class TinyIntCodec extends PrimitiveByteCodec {
        private static final TinyIntCodec instance = new TinyIntCodec();

        private TinyIntCodec() {
            super(DataType.TINYINT);
        }

        public Byte parse(String value) {
            try {
                return value != null && !value.isEmpty() && !value.equalsIgnoreCase("NULL") ? Byte.parseByte(value) : null;
            } catch (NumberFormatException var3) {
                throw new InvalidTypeException(String.format("Cannot parse 8-bits int value from \"%s\"", value));
            }
        }

        public String format(Byte value) {
            return value == null ? "NULL" : Byte.toString(value);
        }

        public Object serializeNoBoxing(byte value) {
            return value;
        }

        public byte deserializeNoBoxing(Object obj) {
            if (obj != null) {
                if (obj instanceof Boolean) {
                    byte trueByte = 1;
                    byte falseByte = 0;
                    return ((boolean) obj) ? trueByte : falseByte;
                }
                if (patternInteger.matcher(obj.toString()).matches()) {
                    return Byte.parseByte(obj.toString());
                }
                throw new InvalidTypeException("Invalid 8-bits integer value, expecting 1 byte but got " + obj.toString());
            } else {
                return 0;
            }
        }
    }

    private static class SmallIntCodec extends PrimitiveShortCodec {
        private static final SmallIntCodec instance = new SmallIntCodec();

        private SmallIntCodec() {
            super(DataType.SMALLINT);
        }

        public Short parse(String value) {
            try {
                return value != null && !value.isEmpty() && !value.equalsIgnoreCase("NULL") ? Short.parseShort(value) : null;
            } catch (NumberFormatException var3) {
                throw new InvalidTypeException(String.format("Cannot parse 16-bits int value from \"%s\"", value));
            }
        }

        public String format(Short value) {
            return value == null ? "NULL" : Short.toString(value);
        }

        public Object serializeNoBoxing(short value) {
            return value;
        }

        public short deserializeNoBoxing(Object obj) {
            if (obj != null) {
                if (!patternInteger.matcher(obj.toString()).matches()) {
                    throw new InvalidTypeException("Invalid 16-bits integer value, expecting 2 bytes but got " + obj.toString());
                } else {
                    return Short.valueOf(obj.toString());
                }
            } else {
                return 0;
            }
        }
    }

    private static class IntCodec extends PrimitiveIntCodec {
        private static final IntCodec instance = new IntCodec();

        private IntCodec() {
            super(DataType.INT);
        }

        public Integer parse(String value) {
            try {
                return value != null && !value.isEmpty() && !value.equalsIgnoreCase("NULL") ? Integer.parseInt(value) : null;
            } catch (NumberFormatException var3) {
                throw new InvalidTypeException(String.format("Cannot parse 32-bits int value from \"%s\"", value));
            }
        }

        public String format(Integer value) {
            return value == null ? "NULL" : Integer.toString(value);
        }

        public Object serializeNoBoxing(int value) {
            return value;
        }

        public int deserializeNoBoxing(Object obj) {
            if (obj != null) {
                if (!patternInteger.matcher(obj.toString()).matches()) {
                    throw new InvalidTypeException("Invalid 32-bits integer value, expecting 4 bytes but got " + obj.toString());
                } else {
                    return Integer.parseInt(obj.toString());
                }
            } else {
                return 0;
            }
        }
    }

    private static class BigintCodec extends PrimitiveBigintCodec {
        private static final BigintCodec instance = new BigintCodec();

        private BigintCodec() {
            super(DataType.BIGINT);
        }

        public Long parse(String value) {
            try {
                return value != null && !value.isEmpty() && !value.equalsIgnoreCase("NULL") ? Long.parseLong(value) : null;
            } catch (NumberFormatException var3) {
                throw new InvalidTypeException(String.format("Cannot parse 64-bits long value from \"%s\"", value));
            }
        }

        public String format(Long value) {
            return value == null ? "NULL" : Long.toString(value);
        }

        public Object serializeNoBoxing(long value) {
            return value;
        }

        public long deserializeNoBoxing(Object obj) {
            if (obj != null) {
                if (!patternInteger.matcher(obj.toString()).matches()) {
                    throw new InvalidTypeException("Invalid 64-bits long value, expecting 8 bytes but got " + obj.toString());
                } else {
                    return Long.parseLong(obj.toString());
                }
            } else {
                return 0L;
            }
        }
    }

    private static class FloatCodec extends PrimitiveFloatCodec {
        private static final FloatCodec instance = new FloatCodec();

        private FloatCodec() {
            super(DataType.FLOAT);
        }

        public Float parse(String value) {
            try {
                return value != null && !value.isEmpty() && !value.equalsIgnoreCase("NULL") ? Float.parseFloat(value) : null;
            } catch (NumberFormatException var3) {
                throw new InvalidTypeException(String.format("Cannot parse 32-bits float value from \"%s\"", value));
            }
        }

        public String format(Float value) {
            return value == null ? "NULL" : Float.toString(value);
        }

        public Object serializeNoBoxing(float value) {
            return value;
        }

        public float deserializeNoBoxing(Object obj) {
            if (obj != null) {
                if (obj instanceof Float) {
                    return (Float) obj;
                } else if (!patternFloat.matcher(obj.toString()).matches()) {
                    throw new InvalidTypeException("Invalid 32-bits float value, expecting 4 bytes but got " + obj.toString());
                } else {
                    return Float.valueOf(obj.toString());
                }
            } else {
                return 0.0F;
            }
        }
    }

    private static class DoubleCodec extends PrimitiveDoubleCodec {
        private static final DoubleCodec instance = new DoubleCodec();

        private DoubleCodec() {
            super(DataType.DOUBLE);
        }

        public Double parse(String value) {
            try {
                return value != null && !value.isEmpty() && !value.equalsIgnoreCase("NULL") ? Double.parseDouble(value) : null;
            } catch (NumberFormatException var3) {
                throw new InvalidTypeException(String.format("Cannot parse 64-bits double value from \"%s\"", value));
            }
        }

        public String format(Double value) {
            return value == null ? "NULL" : Double.toString(value);
        }

        public Object serializeNoBoxing(double value) {
            return value;
        }

        public double deserializeNoBoxing(Object obj) {
            if (obj != null) {
                if (obj instanceof Double) {
                    return (Double) obj;
                } else if (!patternFloat.matcher(obj.toString()).matches()) {
                    throw new InvalidTypeException("Invalid 64-bits double value, expecting 8 bytes but got " + obj.toString());
                } else {
                    return Double.valueOf(obj.toString());
                }
            } else {
                return 0.0D;
            }
        }
    }

    private static class StringCodec extends PrimitiveStringCodec {
        private static final StringCodec instance = new StringCodec();

        private StringCodec() {
            super(DataType.DOUBLE);
        }

        @Override
        public String parse(String value) {
            try {
                return value != null && !value.isEmpty() && !value.equalsIgnoreCase("NULL") ? value : null;
            } catch (NumberFormatException var3) {
                throw new InvalidTypeException(String.format("Cannot parse 64-bits double value from \"%s\"", value));
            }
        }

        @Override
        public String format(String value) {
            return value == null ? "NULL" : value;
        }

        @Override
        public Object serializeNoBoxing(String value) {
            return value;
        }

        @Override
        public String deserializeNoBoxing(Object obj) {
            if (obj != null) {
                if (obj instanceof String) {
                    return (String) obj;
                } else {
                    return String.valueOf(obj);
                }
            } else {
                return null;
            }
        }
    }

    @Override
    public String toString() {
        return String.format("%s [%s <-> %s]", this.getClass().getSimpleName(), cqlType, javaType);
    }
}
