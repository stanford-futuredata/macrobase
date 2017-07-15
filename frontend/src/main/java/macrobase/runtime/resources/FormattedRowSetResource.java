package macrobase.runtime.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.SQLIngester;
import macrobase.ingest.result.RowSet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;

@Path("/rows/fmt")
@Produces(MediaType.APPLICATION_JSON)
public class FormattedRowSetResource extends BaseResource {
    private static final Logger log = LoggerFactory.getLogger(SchemaResource.class);

    public enum RETURNTYPE { JSON, CSV, SQL };

    public static class RowSetRequest {
        public String pgUrl;
        public String baseQuery;
        public List<RowRequestPair> columnValues;
        public int limit;
        public int offset;
        public RETURNTYPE returnType;

        public static class RowRequestPair {
            public String column;
            public String value;
        }
    }

    public static class FormattedRowSetResponse {
        public String response;
        public String errorMessage;
    }

    public FormattedRowSetResource(MacroBaseConf conf) {
        super(conf);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public FormattedRowSetResponse getRowsFormatted(RowSetRequest request) {
        FormattedRowSetResponse response = new FormattedRowSetResponse();

        try {
            conf.set(MacroBaseConf.DB_URL, request.pgUrl);
            conf.set(MacroBaseConf.BASE_QUERY, request.baseQuery);

            HashMap<String, String> preds = new HashMap<>();
            request.columnValues.stream().forEach(a -> preds.put(a.column, a.value));

            if(request.returnType == RETURNTYPE.SQL) {
                response.response = ((SQLIngester) getLoader()).getRowsSql(request.baseQuery,
                                                           preds,
                                                           request.limit,
                                                           request.offset)+";";
                return response;
            }

            RowSet r = getLoader().getRows(request.baseQuery,
                                                  preds,
                                                  request.limit,
                                                  request.offset);


            if (request.returnType == RETURNTYPE.JSON) {
                response.response = new ObjectMapper().writeValueAsString(r);
            } else {
                assert (request.returnType == RETURNTYPE.CSV);
                StringWriter sw = new StringWriter();
                CSVPrinter printer = new CSVPrinter(sw, CSVFormat.DEFAULT);

                if(r.getRows().isEmpty()) {
                    printer.printRecord(preds.keySet());
                } else {
                    printer.printRecord(r.getRows().get(0).getColumnValues().stream().map(a -> a.getColumn()).toArray());
                    for (RowSet.Row row : r.getRows()) {
                        printer.printRecord(row.getColumnValues().stream().map(a -> a.getValue()).toArray());
                    }
                }

                response.response = sw.toString();
            }
        } catch (Exception e) {
            log.error("An error occurred while processing a request:", e);
            response.errorMessage = ExceptionUtils.getStackTrace(e);
        }

        return response;
    }

}
