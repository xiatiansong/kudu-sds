package org.bg.kudu.client.helper;

import org.bg.kudu.annotation.Column;
import org.bg.kudu.annotation.DataType;
import org.bg.kudu.annotation.Entity;
import org.bg.kudu.annotation.codec.Defaults;
import org.bg.kudu.annotation.codec.TypeCodec;
import org.bg.kudu.util.ReflectionUtils;
import org.apache.commons.beanutils.ConstructorUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;

/**
 * Bean设置帮助类
 *
 * @author xiatiansong
 */
public class BeanHelper {

    private static final Log LOG = LogFactory.getLog(BeanHelper.class);

    public static <T> Map<String, Object> toMap(T obj) throws Exception {
        BeanSetDescription beanSetDescription = BeanSetDescription.getBeanSetDescription(obj.getClass());
        Map<String, Object> map = new HashMap<>(16);
        Field[] field = obj.getClass().getDeclaredFields();
        Field[] parentField = obj.getClass().getSuperclass().getDeclaredFields();
        List<Field> fields = new ArrayList<>(Arrays.asList(field));
        fields.addAll(Arrays.asList(parentField));
        for (Field aField : fields) {
            String name = aField.getName();
            Column columnAnnotation = aField.getAnnotation(Column.class);
            String type = aField.getGenericType().toString();
            String columnName = name;
            DataType columnType = null;
            boolean hasCloumnAnnotation = false;
            Class<? extends TypeCodec<?>> codecClass = null;
            if (null != columnAnnotation) {
                hasCloumnAnnotation = true;
                codecClass = columnAnnotation.codec();
                columnName = columnAnnotation.name();
                columnType = columnAnnotation.type();
            }
            Method method;
            method = getMethod(beanSetDescription, name, type);
            if (method != null) {
                Object valueObj = method.invoke(obj);
                fillingMap(map, columnName, columnType, type, valueObj, hasCloumnAnnotation, codecClass);
            }
        }
        return map;
    }

    private static Method getMethod(BeanSetDescription beanSetDescription, String name, String type) {
        Method method;
        if ("boolean".equals(type)) {
            method = beanSetDescription.getIsMethod(name);
            if (null == method) {
                method = beanSetDescription.getGetMethod(name);
            }
        } else {
            method = beanSetDescription.getGetMethod(name);
        }
        return method;
    }

    private static void fillingMap(Map<String, Object> map, String columnName, DataType columnType, String type, Object value, boolean hasCloumnAnnotation, Class<? extends TypeCodec<?>> codecClass) {
        String mapKey = columnName;
        if (!hasCloumnAnnotation) {
            mapKey = humpToUnderline(columnName);
        }
        if (null != value && value.toString().length() > 0) {
            if (null == columnType || DataType.DEFAULT == columnType) {
                map.put(mapKey, value);
            } else {
                map.put(mapKey, toColumnType(value, type, columnType, codecClass));
            }
        }
    }

    private static Object toColumnType(Object value, String type, DataType columnType, Class<? extends TypeCodec<?>> codecClass) {
        if (type.equals(columnType.getClazz().toGenericString())) {
            return value;
        } else {
            if (null == codecClass || Defaults.NoCodec.class.equals(codecClass)) {
                switch (columnType) {
                    case TINYINT:
                        return TypeCodec.tinyInt().deserialize(value);
                    case SMALLINT:
                        return TypeCodec.smallInt().deserialize(value);
                    case INT:
                        return TypeCodec.kint().deserialize(value);
                    case BIGINT:
                        return TypeCodec.bigint().deserialize(value);
                    case FLOAT:
                        return TypeCodec.kfloat().deserialize(value);
                    case DOUBLE:
                        return TypeCodec.kdouble().deserialize(value);
                    case BOOLEAN:
                        return TypeCodec.kboolean().deserialize(value);
                    case STRING:
                        return toValue(value, codecClass);
                    default:
                        return value;
                }
            } else {
                return toValue(value, codecClass);
            }
        }
    }

    private static <T> Object toValue(T value, Class<? extends TypeCodec<?>> codecClass) {
        if (null == codecClass || codecClass.equals(Defaults.NoCodec.class)) {
            return value;
        } else {
            TypeCodec codec = ReflectionUtils.newInstance(codecClass);
            return codec.serialize(value);
        }
    }

    private static Object fromColumnType(Object obj, DataType annotationType, Type fieldType, Class<? extends TypeCodec<?>> codecClass) {
        Object result;

        if (!Defaults.NoCodec.class.equals(codecClass)) {
            TypeCodec typeCodec = ReflectionUtils.newInstance(codecClass);
            result = typeCodec.deserialize(obj);
        } else {
            if (null == obj) {
                return null;
            } else {
                if (String.class.equals(fieldType)) {
                    result = obj.toString();
                } else if (Long.class.equals(fieldType) || long.class.equals(fieldType)) {
                    result = TypeCodec.bigint().deserialize(obj);
                } else if (Integer.class.equals(fieldType) || int.class.equals(fieldType)) {
                    result = TypeCodec.kint().deserialize(obj);
                } else if (Short.class.equals(fieldType) || short.class.equals(fieldType)) {
                    result = TypeCodec.smallInt().deserialize(obj);
                } else if (Float.class.equals(fieldType) || float.class.equals(fieldType)) {
                    result = TypeCodec.kfloat().deserialize(obj);
                } else if (Double.class.equals(fieldType) || double.class.equals(fieldType)) {
                    result = TypeCodec.kdouble().deserialize(obj);
                } else if (Boolean.class.equals(fieldType) || boolean.class.equals(fieldType)) {
                    result = TypeCodec.kboolean().deserialize(obj);
                } else if (Byte.class.equals(fieldType) || byte.class.equals(fieldType)) {
                    result = TypeCodec.tinyInt().deserialize(obj);
                } else if (BigDecimal.class.equals(fieldType)) {
                    result = TypeCodec.decimal().deserialize(obj);
                } else {
                    result = obj;
                }
            }
        }
        return result;
    }


    private static String humpToUnderline(String para) {
        StringBuilder sb = new StringBuilder(para);
        int temp = 0;//定位
        for (int i = 0; i < para.length(); i++) {
            if (Character.isUpperCase(para.charAt(i))) {
                char theChar = para.charAt(i);
                theChar = Character.toLowerCase(theChar);
                sb.deleteCharAt(i + temp);
                sb.insert(i + temp, "_" + theChar);
                temp += 1;
            }
        }
        return sb.toString();
    }

    public static <T> T toObject(Map<String, Object> data, Class<T> clazz) throws Exception {
        BeanSetDescription<T> beanSetDescription = BeanSetDescription.getBeanSetDescription(clazz);
        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (null != data && data.size() != 0) {
            if (null != entityAnnotation) {
                T result = ConstructorUtils.invokeConstructor(beanSetDescription.clazz, new Object[]{});
                populateByAnnotation(result, data, beanSetDescription);
                return result;
            } else {
                T result = ConstructorUtils.invokeConstructor(beanSetDescription.clazz, new Object[]{});
                populate(result, data, beanSetDescription);
                return result;

            }
        }
        return null;
    }

    private static <T> void populateByAnnotation(T result, Map<String, Object> data, BeanSetDescription beanSetDescription) throws IllegalAccessException, InvocationTargetException {
        if (result == null || data == null) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("BeanUtils.populateByAnnotation(" + result + ", " + data + ")");
        }
        Field[] field = result.getClass().getDeclaredFields();
        Field[] parentField = result.getClass().getSuperclass().getDeclaredFields();
        List<Field> fields = new ArrayList<>(Arrays.asList(field));
        fields.addAll(Arrays.asList(parentField));
        for (Field oneField : fields) {
            Column column = oneField.getAnnotation(Column.class);
            Type fieldType = oneField.getGenericType();
            if (null != column) {
                String name = oneField.getName();
                String columnName = column.name();
                DataType annotationType = column.type();
                Class<? extends TypeCodec<?>> codecClass = column.codec();
                Object value = data.get(columnName);
                Method setMethod = beanSetDescription.getSetMethod(name);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(name);
                }
                Object resultValue = fromColumnType(value, annotationType, fieldType, codecClass);
                if (null != setMethod && null != resultValue) {
                    setMethod.invoke(result, resultValue);
                }
            }
        }
    }


    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void populate(Object bean, Map<String, Object> data, BeanSetDescription beanSetDescription) throws IllegalAccessException, InvocationTargetException {
        // Do nothing unless both arguments have been specified
        if (bean == null || data == null) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("BeanUtils.populate(" + bean + ", " + data + ")");
        }
        // Loop through the property name/value pairs to be set
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            // Identify the property name and value(s) to be assigned
            String name = entry.getKey();
            if (name == null) {
                continue;
            }
            if (Map.class.isAssignableFrom(bean.getClass())) {
                ((Map) bean).put(name, entry.getValue());
            } else {
                Method setMethod = beanSetDescription.getSetMethod(entry.getKey());
                if (null != setMethod) {
                    setMethod.invoke(bean, entry.getValue());
                }
            }
        }
    }

    public static <T> Map<String, Object> toPrimaryKeyMap(T obj) throws Exception {
        BeanSetDescription beanSetDescription = BeanSetDescription.getBeanSetDescription(obj.getClass());
        Map<String, Object> primaryKeyMap = new HashMap<>(8);
        Field[] field = obj.getClass().getDeclaredFields();
        Field[] parentField = obj.getClass().getSuperclass().getDeclaredFields();
        List<Field> fields = new ArrayList<>(Arrays.asList(field));
        fields.addAll(Arrays.asList(parentField));
        for (Field filed : fields) {
            Column columnAnnotation = filed.getAnnotation(Column.class);
            if (null != columnAnnotation && columnAnnotation.primaryKey()) {
                Class<? extends TypeCodec<?>> codecClass = columnAnnotation.codec();
                String type = filed.getGenericType().toString();
                String columnName = columnAnnotation.name();
                DataType columnType = columnAnnotation.type();
                String name = filed.getName();
                Method method = getMethod(beanSetDescription, name, type);
                if (method != null) {
                    Object valueObj = method.invoke(obj);
                    fillingMap(primaryKeyMap, columnName, columnType, type, valueObj, true, codecClass);
                }
            }
        }
        return primaryKeyMap;
    }
}