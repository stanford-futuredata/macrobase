package macrobase.runtime.resources;

import macrobase.ingest.SQLLoader;
import macrobase.ingest.result.RowSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/rows")
@Produces(MediaType.APPLICATION_JSON)
public class RowSetResource {
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

    private SQLLoader loader;

    public RowSetResource(SQLLoader _loader) {
        loader = _loader;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public RowSet getRows(RowSetRequest request) throws Exception {
        loader.connect(request.pgUrl);
        return loader.getRows(request.baseQuery,
                              request.columnValues,
                              request.limit,
                              request.offset);
    }
}
