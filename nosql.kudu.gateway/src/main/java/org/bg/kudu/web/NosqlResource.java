package org.bg.kudu.web;

import org.bg.kudu.core.KuduOptHelper;
import org.bg.kudu.core.lib.KuduOp;
import org.bg.kudu.model.ScanRequest;
import org.bg.kudu.util.GwConstants;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * nosql kudu操作类
 */
@Path("/nosql")
public class NosqlResource {

    @POST
    @Path("/{tableName}/get")
    @Consumes("application/json")
    @Produces("application/json")
    public Response get(@PathParam("tableName") String tableName, Map<String, Object> row) {
        if (!validateTbl(tableName) || Objects.isNull(row) || row.isEmpty()) {
            return GwConstants.processResponse(Response.Status.BAD_REQUEST, "", GwConstants.ERROR_MSG);
        }
        return GwConstants.processResponse(Response.Status.OK, KuduOptHelper.get(tableName, row), GwConstants.OK_MSG);
    }

    @POST
    @Path("/{tableName}/gets")
    @Consumes("application/json")
    @Produces("application/json")
    public Response gets(@PathParam("tableName") String tableName, List<Map<String, Object>> rows) {
        if (!validateTbl(tableName) || Objects.isNull(rows) || rows.isEmpty()) {
            return GwConstants.processResponse(Response.Status.BAD_REQUEST, "", GwConstants.ERROR_MSG);
        }
        return GwConstants.processResponse(Response.Status.OK, KuduOptHelper.gets(tableName, rows), GwConstants.OK_MSG);
    }

    @POST
    @Path("/{tableName}/insert")
    @Consumes("application/json")
    @Produces("application/json")
    public Response insert(@PathParam("tableName") String tableName, Map<String, Object> row) {
        if (!validateTbl(tableName) || Objects.isNull(row) || row.isEmpty()) {
            return GwConstants.processResponse(Response.Status.BAD_REQUEST, "", GwConstants.ERROR_MSG);
        }
        KuduOptHelper.cudOperate(tableName, KuduOp.INSERT, row);
        return GwConstants.processResponse(Response.Status.OK, "", GwConstants.OK_MSG);
    }

    @POST
    @Path("/{tableName}/upsert")
    @Consumes("application/json")
    @Produces("application/json")
    public Response upsert(@PathParam("tableName") String tableName, Map<String, Object> row) {
        if (!validateTbl(tableName) || Objects.isNull(row) || row.isEmpty()) {
            return GwConstants.processResponse(Response.Status.BAD_REQUEST, "", GwConstants.ERROR_MSG);
        }
        KuduOptHelper.cudOperate(tableName, KuduOp.UPSERT, row);
        return GwConstants.processResponse(Response.Status.OK, "", GwConstants.OK_MSG);
    }

    @POST
    @Path("/{tableName}/inserts")
    @Consumes("application/json")
    @Produces("application/json")
    public Response inserts(@PathParam("tableName") String tableName, List<Map<String, Object>> rows) {
        if (!validateTbl(tableName) || Objects.isNull(rows) || rows.isEmpty()) {
            return GwConstants.processResponse(Response.Status.BAD_REQUEST, "", GwConstants.ERROR_MSG);
        }
        KuduOptHelper.cudOperates(tableName, KuduOp.INSERT, rows);
        return GwConstants.processResponse(Response.Status.OK, "", GwConstants.OK_MSG);
    }

    @POST
    @Path("/{tableName}/upserts")
    @Consumes("application/json")
    @Produces("application/json")
    public Response upserts(@PathParam("tableName") String tableName, List<Map<String, Object>> rows) {
        if (!validateTbl(tableName) || Objects.isNull(rows) || rows.isEmpty()) {
            return GwConstants.processResponse(Response.Status.BAD_REQUEST, "", GwConstants.ERROR_MSG);
        }
        KuduOptHelper.cudOperates(tableName, KuduOp.UPSERT, rows);
        return GwConstants.processResponse(Response.Status.OK, "", GwConstants.OK_MSG);
    }

    @POST
    @Path("/{tableName}/delete")
    @Consumes("application/json")
    @Produces("application/json")
    public Response delete(@PathParam("tableName") String tableName, Map<String, Object> row) {
        if (!validateTbl(tableName) || Objects.isNull(row) || row.isEmpty()) {
            return GwConstants.processResponse(Response.Status.BAD_REQUEST, "", GwConstants.ERROR_MSG);
        }
        KuduOptHelper.cudOperate(tableName, KuduOp.DELETE, row);
        return GwConstants.processResponse(Response.Status.OK, "", GwConstants.OK_MSG);
    }

    @POST
    @Path("/{tableName}/deletes")
    @Consumes("application/json")
    @Produces("application/json")
    public Response deletes(@PathParam("tableName") String tableName, List<Map<String, Object>> rows) {
        if (!validateTbl(tableName) || Objects.isNull(rows) || rows.isEmpty()) {
            return GwConstants.processResponse(Response.Status.BAD_REQUEST, "", GwConstants.ERROR_MSG);
        }
        KuduOptHelper.cudOperates(tableName, KuduOp.DELETE, rows);
        return GwConstants.processResponse(Response.Status.OK, "", GwConstants.OK_MSG);
    }

    @POST
    @Path("/{tableName}/scan")
    @Consumes("application/json")
    @Produces("application/json")
    public Response scan(@PathParam("tableName") String tableName, ScanRequest sr) {
        if (!validateTbl(tableName)) {
            return GwConstants.processResponse(Response.Status.BAD_REQUEST, "", GwConstants.ERROR_MSG);
        }
        if (!Objects.isNull(sr) && !Objects.isNull(sr.getParams())) {
            //predicates params
            List<Map<String, Object>> data = KuduOptHelper.scanPredicates(tableName, sr.getParams(), sr.getLimit(), sr.isOrdered());
            return GwConstants.processResponse(Response.Status.OK, data, GwConstants.OK_MSG);
        } else if (!Objects.isNull(sr) && !Objects.isNull(sr.getStartParam()) && !Objects.isNull(sr.getEndParam())) {
            //start and end params
            List<Map<String, Object>> data = KuduOptHelper.scanWithStartEndParam(tableName, sr.getStartParam(), sr.getEndParam(), sr.getLimit(), sr.isOrdered());
            return GwConstants.processResponse(Response.Status.OK, data, GwConstants.OK_MSG);
        } else {
            return GwConstants.processResponse(Response.Status.BAD_REQUEST, "", GwConstants.ERROR_MSG);
        }
    }

    private boolean validateTbl(String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            return false;
        }
        return true;
    }

}