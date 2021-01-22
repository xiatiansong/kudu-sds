package org.bg.kudu.client.helper;

import com.google.common.base.CaseFormat;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class BeanSetDescription<T> {
    Class<T> clazz;
    private Map<String, Method> setMethodMap = new HashMap<>();
    private Map<String, Method> getMethodMap = new HashMap<>();

    public static <T> BeanSetDescription<T> getBeanSetDescription(Class<T> clazz) {
        BeanSetDescription<T> beanSetDescription = new BeanSetDescription<>();
        beanSetDescription.clazz = clazz;
        return beanSetDescription;
    }

    Method getGetMethod(String key) {
        Method method = getMethodMap.get(key);
        if (null == method) {
            Method[] methods = clazz.getMethods();
            String getMethodName = "get" + firstToUpperCase(key);
            if (key.contains("_")) {
                getMethodName = "get" + CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, key);
            }
            for (Method theMethod : methods) {
                if (theMethod.getName().equals(getMethodName)) {
                    getMethodMap.put(key, theMethod);
                    return theMethod;
                }
            }
        }
        return method;
    }


    Method getSetMethod(String key) {
        Method method = setMethodMap.get(key);
        if (null == method) {
            Method[] methods = clazz.getMethods();
            String setMethodName = "set" + firstToUpperCase(key);
            if (key.contains("_")) {
                setMethodName = "set" + CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, key);
            }
            for (Method theMethod : methods) {
                if (theMethod.getName().equals(setMethodName)) {
                    setMethodMap.put(key, theMethod);
                    return theMethod;
                }
            }
        }
        return method;
    }

    private static String firstToUpperCase(String str) {
        if (null == str || str.length() == 0) {
            return str;
        }
        StringBuilder buf = new StringBuilder();
        buf.append(Character.toUpperCase(str.charAt(0)));
        if (str.length() > 1) {
            buf.append(str.substring(1));
        }
        return buf.toString();
    }

    public Method getIsMethod(String key) {
        Method method = getMethodMap.get(key);
        if (null == method) {
            Method[] methods = clazz.getMethods();
            String getMethodName = "is" + firstToUpperCase(key);
            if (key.contains("_")) {
                getMethodName = "is" + CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, key);
            }
            for (Method theMethod : methods) {
                if (theMethod.getName().equals(getMethodName)) {
                    getMethodMap.put(key, theMethod);
                    return theMethod;
                }
            }
        }
        return method;
    }
}