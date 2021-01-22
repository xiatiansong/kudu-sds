package org.bg.kudu.util;

import java.util.regex.Pattern;

/**
 * 常量
 *
 * @author xiatiansong
 */
public final class Constants {

    public static final int MAX_LIMIT = Integer.MAX_VALUE;

    public static final String DEFAULT = "default";

    /**
     * kudu 常量
     **/
    public interface KuduConstants {

        int SCAN_LIMIT_NUM = 1024;

        /**
         * The start row to scan on empty search strings.  `!' = first ASCII char.
         */
        byte[] START_ROW = new byte[]{(byte) 1};

        /**
         * The end row to scan on empty search strings.  `~' = last ASCII char.
         */
        byte[] STOP_ROW = new byte[]{(byte) 126};
    }

    /**
     * impala 常量
     **/
    public interface ImpalaConstants {

        Pattern QUERY_CMD_PATTERN = Pattern.compile("\\A\\s*([a-zA-Z]+)\\s+(.*)", Pattern.DOTALL);

        Pattern USE_PATTERN = Pattern.compile("\\A\\s*([a-zA-Z]+)\\s+(\\w+)\\s*(;)?", Pattern.DOTALL);

        Pattern SET_PATTERN = Pattern.compile("\\A\\s*([a-zA-Z]+)\\s+(\\w+)\\s*=\"(.*)\"(;)?", Pattern.DOTALL);

        Pattern INSERT_INTO_VALUES_PATTERN = Pattern.compile("\\A\\s*insert\\s+into\\s+\\w+\\s+values\\s+.*", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    }
}