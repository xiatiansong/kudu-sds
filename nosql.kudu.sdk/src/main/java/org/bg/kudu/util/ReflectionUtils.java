package org.bg.kudu.util;

import java.lang.reflect.Constructor;

/**
 * @author: xiatiansong
 * @create: 2019-01-03 09:57
 **/
public class ReflectionUtils {

    public static <T> T newInstance(Class<T> clazz) {
        Constructor<T> publicConstructor;
        try {
            publicConstructor = clazz.getConstructor();
        } catch (NoSuchMethodException e) {
            try {
                // try private constructor
                Constructor<T> privateConstructor = clazz.getDeclaredConstructor();
                privateConstructor.setAccessible(true);
                return privateConstructor.newInstance();
            } catch (Exception e1) {
                throw new IllegalArgumentException("Can't create an instance of " + clazz, e);
            }
        }
        try {
            return publicConstructor.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Can't create an instance of " + clazz, e);
        }
    }
}