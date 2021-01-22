package org.bg.kudu.web;

import org.bg.kudu.util.GwConstants;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.HashMap;
import java.util.Map;

/**
 * 该注解的作用是用于表明该类是javax.ws.rs的一种组件
 */
@Provider
public class GatewayExceptionMapper implements ExceptionMapper<Throwable> {

    @Override
    public Response toResponse(Throwable e) {
        Map<String, Object> json = new HashMap<String, Object>();
        json.put(GwConstants.RETURN_CODE, Response.Status.BAD_REQUEST.getStatusCode());
        json.put(GwConstants.RETURN_MSG, e.getMessage());
        json.put(GwConstants.RETURN_DATA, "");
        return Response.status(Response.Status.BAD_REQUEST).entity(json).build();
    }
}
