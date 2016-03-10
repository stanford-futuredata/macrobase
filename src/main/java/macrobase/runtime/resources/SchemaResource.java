package macrobase.runtime.resources;

import macrobase.conf.MacroBaseConf;
import macrobase.ingest.result.Schema;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/schema")
@Produces(MediaType.APPLICATION_JSON)
public class SchemaResource extends BaseResource {
    static class SchemaRequest {
        public String pgUrl;
        public String baseQuery;
    }

    public SchemaResource(MacroBaseConf conf) {
        super(conf);
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    public Schema getSchema(SchemaRequest request) throws Exception {
        conf.set(MacroBaseConf.DB_URL, request.pgUrl);
        conf.set(MacroBaseConf.BASE_QUERY, request.baseQuery);

        return getLoader().getSchema(request.baseQuery);
    }
}
