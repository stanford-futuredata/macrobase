package macrobase.runtime.resources;

import macrobase.conf.MacroBaseConf;
import macrobase.ingest.result.RowSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

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

    public RowSetResource(MacroBaseConf conf) {
        super(conf);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public RowSet getRows(RowSetRequest request) throws Exception {
        conf.set(MacroBaseConf.DB_URL, request.pgUrl);
        return getLoader().getRows(request.baseQuery,
                                   request.columnValues,
                                   request.limit,
                                   request.offset);
    }
}
