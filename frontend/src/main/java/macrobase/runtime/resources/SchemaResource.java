package macrobase.runtime.resources;

import macrobase.conf.MacroBaseConf;
import macrobase.ingest.result.Schema;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/schema")
@Produces(MediaType.APPLICATION_JSON)
public class SchemaResource extends BaseResource {
    private static final Logger log = LoggerFactory.getLogger(SchemaResource.class);

    static class SchemaRequest {
        public String pgUrl;
        public String baseQuery;
    }

    static class SchemaResponse {
        public Schema schema;
        public String errorMessage;
    }

    public SchemaResource(MacroBaseConf conf) {
        super(conf);
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    public SchemaResponse getSchema(SchemaRequest request) {
        SchemaResponse response = new SchemaResponse();
        try {
            conf.set(MacroBaseConf.DB_URL, request.pgUrl);
            conf.set(MacroBaseConf.BASE_QUERY, request.baseQuery);
            response.schema = getLoader().getSchema(request.baseQuery);
        } catch (Exception e) {
            log.error("An error occurred while processing a request: {}", e);
            response.errorMessage = ExceptionUtils.getStackTrace(e);
        }

        return response;
    }
}
