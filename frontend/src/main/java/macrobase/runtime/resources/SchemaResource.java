package macrobase.runtime.resources;

import macrobase.conf.MacroBaseConf;
import macrobase.ingest.result.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;

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
            // temp hack to enable CSV loading
            if(request.baseQuery.contains("csv://")) {
                File csvFile = new File(request.baseQuery.replace("csv://", ""));
                CSVParser p = CSVParser.parse(csvFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());

                Schema s = new Schema(new ArrayList<>());
                for(String header : p.getHeaderMap().keySet()) {
                    s.getColumns().add(new Schema.SchemaColumn(header, "entry"));
                }
                response.schema = s;
            } else {
                conf.set(MacroBaseConf.DB_URL, request.pgUrl);
                conf.set(MacroBaseConf.BASE_QUERY, request.baseQuery);
                response.schema = getLoader().getSchema(request.baseQuery);
            }
        } catch (Exception e) {
            log.error("An error occurred while processing a request:", e);
            response.errorMessage = ExceptionUtils.getStackTrace(e);
        }

        return response;
    }
}
