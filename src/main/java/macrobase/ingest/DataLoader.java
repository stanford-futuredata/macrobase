package macrobase.ingest;

import macrobase.datamodel.Datum;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import macrobase.ingest.transform.DataTransformation;
import macrobase.runtime.resources.RowSetResource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public abstract class DataLoader {


    public abstract Schema getSchema(String baseQuery)
            throws SQLException, IOException;

    public abstract List<Datum> getData(DatumEncoder encoder,
                                        List<String> attributes,
                                        List<String> lowMetrics,
                                        List<String> highMetrics,
                                        List<String> auxiliaryAttributes,
                                        DataTransformation dataTransform,
                                        String baseQuery)
            throws SQLException, IOException;

    public abstract RowSet getRows(String baseQuery,
                          List<RowSetResource.RowSetRequest.RowRequestPair> preds,
                          int limit,
                          int offset)
            throws SQLException, IOException;

    public abstract void connect(String source)
            throws SQLException, IOException;

    public abstract void setDatabaseCredentials(String user, String password);
}
