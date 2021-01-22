package org.bg.kudu.util;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

/**
 * 常量
 *
 * @author xiatiansong
 */
public final class GwConstants {

    public static final String RETURN_CODE = "code";

    public static final String RETURN_DATA = "data";

    public static final String RETURN_MSG = "msg";

    public static final String ERROR_MSG = "error table name or empty data body";

    public static final String OK_MSG = "request success";

    /**
     * 处理返回
     *
     * @param status
     * @param data
     * @param msg
     * @return
     */
    public static Response processResponse(Response.Status status, Object data, String msg) {
        Map<String, Object> json = new HashMap<String, Object>();
        json.put(GwConstants.RETURN_CODE, status.getStatusCode());
        json.put(GwConstants.RETURN_MSG, msg);
        json.put(GwConstants.RETURN_DATA, data);
        return Response.status(status).entity(json).build();
    }
}