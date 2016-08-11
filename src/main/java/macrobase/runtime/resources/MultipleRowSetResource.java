package macrobase.runtime.resources;

import macrobase.conf.MacroBaseConf;
import macrobase.ingest.DataIngester;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.SQLIngester;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

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

    public MultipleRowSetResource(MacroBaseConf conf) {
        super(conf);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public List<RowSet> getRows(MultipleRowSetRequest request) throws Exception {
        conf.set(MacroBaseConf.DB_URL, request.pgUrl);
        DataIngester loader = getLoader();
        List<RowSet> lr = new ArrayList<RowSet>();
        for(List<RowSetResource.RowSetRequest.RowRequestPair> columnValue : request.columnValues){
            lr.add(loader.getRows(request.baseQuery,
                        columnValue,
                        request.limit,
                        request.offset));
        }

        return lr;
    }
}
