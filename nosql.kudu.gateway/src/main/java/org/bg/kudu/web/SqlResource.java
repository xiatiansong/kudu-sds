package org.bg.kudu.web;

import org.bg.kudu.core.ImpalaOptHelper;
import org.bg.kudu.model.SqlRequest;
import org.bg.kudu.util.GwConstants;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * sql 操作类
 */
@Path("/sql")
public class SqlResource {

    @POST
    @Path("/opt")
    @Consumes("application/json")
    @Produces("application/json")
    public Response sqlOpt(SqlRequest req) {
        if (!validateDbOpt(req)) {
            return GwConstants.processResponse(Response.Status.BAD_REQUEST, "", GwConstants.ERROR_MSG);
        }
        try {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put("rowsAffected", ImpalaOptHelper.getInstance().impalaOpt(req.getDatabase(), req.getSql()));
            return GwConstants.processResponse(Response.Status.OK, ret, GwConstants.OK_MSG);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @POST
    @Path("/query")
    @Consumes("application/json")
    @Produces("application/json")
    public Response sqlQuery(SqlRequest req) {
        if (!validateDbOpt(req)) {
            return GwConstants.processResponse(Response.Status.BAD_REQUEST, "", GwConstants.ERROR_MSG);
        }
        try {
            return GwConstants.processResponse(Response.Status.OK, ImpalaOptHelper.getInstance().impalaQuery(req.getDatabase(), req.getSql()), GwConstants.OK_MSG);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean validateDbOpt(SqlRequest sr) {
        if (Objects.isNull(sr) || StringUtils.isEmpty(sr.getSql())) {
            return false;
        }
        return true;
    }
}