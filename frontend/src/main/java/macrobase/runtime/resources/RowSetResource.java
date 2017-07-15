package macrobase.runtime.resources;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.ingest.result.RowSet;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.util.HashMap;
import java.util.List;

@Path("/rows")
@Produces(MediaType.APPLICATION_JSON)
public class RowSetResource extends BaseResource {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(SchemaResource.class);

    public static class RowSetRequest {
        public String pgUrl;
        public String baseQuery;
        public List<RowRequestPair> columnValues;
        public int limit;
        public int offset;

        public static class RowRequestPair {
            public String column;
            public String value;
        }
    }

    public static class RowSetResponse {
        public RowSet rowSet;
        public String errorMessage;
    }

    public RowSetResource(MacroBaseConf conf) {
        super(conf);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public RowSetResponse getRows(RowSetRequest request) {
        RowSetResponse response = new RowSetResponse();

        try {
            conf.set(MacroBaseConf.DB_URL, request.pgUrl);
            conf.set(MacroBaseConf.BASE_QUERY, request.baseQuery);
            configuredIngester = conf.getString(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseDefaults.DATA_LOADER_TYPE.toString());

            HashMap<String, String> preds = new HashMap<>();
            request.columnValues.stream().forEach(a -> preds.put(a.column, a.value));

            response.rowSet = getLoader().getRows(request.baseQuery,
                                                  preds,
                                                  request.limit,
                                                  request.offset);
        } catch (Exception e) {
            log.error("An error occurred while processing a request:", e);
            response.errorMessage = ExceptionUtils.getStackTrace(e);
        }

        return response;
    }
}
