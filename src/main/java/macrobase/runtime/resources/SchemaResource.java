package macrobase.runtime.resources;

import macrobase.ingest.SQLLoader;
import macrobase.ingest.result.Schema;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/schema")
@Produces(MediaType.APPLICATION_JSON)
public class SchemaResource {
    @SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger(SchemaResource.class);

    static class SchemaRequest {
        public String pgUrl;
        public String baseQuery;
    }

    private SQLLoader loader;

    public SchemaResource(SQLLoader _loader) {
        loader = _loader;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    public Schema getSchema(SchemaRequest request) throws Exception {
        loader.connect(request.pgUrl);
        return loader.getSchema(request.baseQuery);
    }
}
