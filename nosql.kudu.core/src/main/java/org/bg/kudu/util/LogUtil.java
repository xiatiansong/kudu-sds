package org.bg.kudu.util;


import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Arrays;

/**
 * 日志工具类
 *
 * @author xiatiansong
 */
public class LogUtil {

    /**
     * 获取打印堆栈所有信息
     *
     * @param throwable
     * @return
     */
    public static String getAllStackTrace(Throwable throwable) {
        return Arrays.toString(ExceptionUtils.getStackFrames(throwable));
    }

    /**
     * 获取打印堆栈信息
     *
     * @param throwable
     * @return
     */
    public static String getStackTrace(Throwable throwable) {
        return ExceptionUtils.getStackTrace(throwable);
    }
}