package macrobase.runtime.resources;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.ingest.CSVIngester;
import macrobase.ingest.DataIngester;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.SQLIngester;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

@Path("/rows/multiple")
@Produces(MediaType.APPLICATION_JSON)
public class MultipleRowSetResource extends BaseResource {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(SchemaResource.class);

    public static class MultipleRowSetRequest {
        public String pgUrl;
        public String baseQuery;
        public List<List<RowSetResource.RowSetRequest.RowRequestPair>> columnValues;
        public int limit;
        public int offset;
    }

    public static class MultipleRowSetResponse {
        public List<RowSet> rowSets;
        public String errorMessage;
    }

    public MultipleRowSetResource(MacroBaseConf conf) {
        super(conf);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public MultipleRowSetResponse getRows(MultipleRowSetRequest request) {
        MultipleRowSetResponse response = new MultipleRowSetResponse();

        try {
            conf.set(MacroBaseConf.DB_URL, request.pgUrl);

            // Need a CSV ingester. Changing so it's not fixed during construction (used to be final, but changed as
            // this is set in AnalyzeResource after construction)
            configuredIngester = conf.getString(MacroBaseConf.DATA_LOADER_TYPE, MacroBaseDefaults.DATA_LOADER_TYPE.toString());

            DataIngester loader = getLoader();
            List<RowSet> lr = new ArrayList<RowSet>();
            for (List<RowSetResource.RowSetRequest.RowRequestPair> columnValue : request.columnValues) {
                HashMap<String, String> preds = new HashMap<>();
                columnValue.stream().forEach(a -> preds.put(a.column, a.value));
                lr.add(loader.getRows(request.baseQuery,
                                      preds,
                                      request.limit,
                                      request.offset));
            }

            response.rowSets = lr;
        } catch (Exception e) {
            log.error("An error occurred while processing a request:", e);
            response.errorMessage = ExceptionUtils.getStackTrace(e);
        }

        return response;
    }
}
