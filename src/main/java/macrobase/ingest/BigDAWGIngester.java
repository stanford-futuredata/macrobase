package macrobase.ingest;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import macrobase.runtime.resources.RowSetResource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BigDAWGIngester extends DataIngester {
    private static final Logger log = LoggerFactory.getLogger(BigDAWGIngester.class);

    private Connection connection;
    private final String dbUrl;
    private String dbName;
    private final Integer timeColumn;

    protected final String baseQuery;
    protected ResultSet resultSet;


    private final MBStream<Datum> output = new MBStream<>();
    private boolean connected = false;

    private static final String LIMIT_REGEX = "(LIMIT\\s\\d+)";

    public BigDAWGIngester(MacroBaseConf conf) throws ConfigurationException, SQLException {
        this(conf, null);
    }

    @Override
    public String getBaseQuery() {
        return null;
    }

    public BigDAWGIngester(MacroBaseConf conf, Connection connection) throws ConfigurationException, SQLException {
        super(conf);

        baseQuery = conf.getString(MacroBaseConf.BASE_QUERY);
        dbUrl = conf.getString(MacroBaseConf.DB_URL, MacroBaseDefaults.DB_URL);
        timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);

        if (connection != null) {
            this.connection = connection;
        }
    }

    private CSVParser query(String sql) throws IOException {
        String bodyString = String.format("bdrel(%s);", sql);
        log.debug("bigdawg query: {}", bodyString);

        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(dbUrl);
        httpPost.setEntity(new StringEntity(bodyString));
        CloseableHttpResponse response = httpclient.execute(httpPost);
        log.debug("{}", response.toString());

        InputStream responseStream = response.getEntity().getContent();
        Reader streamReader = new InputStreamReader(responseStream);
        CSVParser csvParser = new CSVParser(streamReader, CSVFormat.TDF.withHeader());
        log.debug("headerMap: {}", csvParser.getHeaderMap());

        return csvParser;
    }

    public void connect() throws ConfigurationException, SQLException {
        //initializeResultSet();

        // while(!resultSet.isLast()) {
        //     output.add(getNext());
        // }
    }

    private String removeLimit(String sql) {
        return sql.replaceAll(LIMIT_REGEX, "");
    }

    private String removeSqlJunk(String sql) {
        return sql.replaceAll(";", "");
    }

    @Override
    public Schema getSchema(String baseQuery) throws Exception {


        String sql= String.format("%s LIMIT 1", removeSqlJunk(removeLimit(baseQuery)));

        CSVParser csvParser = query(sql);

        List<Schema.SchemaColumn> columns = Lists.newArrayList();
        for (String name: csvParser.getHeaderMap().keySet()) {
            columns.add(new Schema.SchemaColumn(name, "????"));
        }

        return new Schema(columns);
    }

    @Override
    public RowSet getRows(String baseQuery,
                          List<RowSetResource.RowSetRequest.RowRequestPair> preds,
                          int limit,
                          int offset) throws Exception {
        // initializeConnection();
        // TODO handle time column here
        String sql = removeSqlJunk(removeLimit(baseQuery));

        if (preds.size() > 0) {
            StringJoiner sj = new StringJoiner(" AND ");
            preds.stream().forEach(e -> sj.add(String.format("%s = '%s'", e.column, e.value)));

            if (!sql.toLowerCase().contains("where")) {
                sql += " WHERE ";
            } else {
                sql += " AND ";
            }

            sql += sj.toString();
        }

        sql += String.format(" LIMIT %d OFFSET %d", limit, offset);

        CSVParser csvParser = query(sql);

        Map<String, Integer> schema;
        schema = csvParser.getHeaderMap();

        Map<Integer, String> reverseSchema = new HashMap<>();
        for(Map.Entry<String, Integer> entry : schema.entrySet()){
            reverseSchema.put(entry.getValue(), entry.getKey());
        }

        for (Map.Entry<String, Integer> se : schema.entrySet()) {
            conf.getEncoder().recordAttributeName(se.getValue() + 1, se.getKey());
        }

        // Load all records into memory to filter out rows with missing data
        Iterator<CSVRecord> rawIterator = csvParser.iterator();

        List<RowSet.Row> rows = Lists.newArrayList();
        while (rawIterator.hasNext()) {
            CSVRecord record = rawIterator.next();
            List<ColumnValue> columnValues = Lists.newArrayList();
            for (int i=0; i < schema.size(); i++) {
                columnValues.add(
                        new ColumnValue(reverseSchema.get(i), record.get(i))
                );
            }
            rows.add(new RowSet.Row(columnValues));
        }

        return new RowSet(rows);
    }


    @Override
    public MBStream<Datum> getStream() throws Exception {

        String targetColumns = StreamSupport.stream(
                Iterables.concat(attributes, lowMetrics, highMetrics, contextualDiscreteAttributes,
                        contextualDoubleAttributes, auxiliaryAttributes).spliterator(), false)
                .collect(Collectors.joining(", "));
        if (timeColumn != null) {
            targetColumns += ", " + timeColumn;
        }
        String sql = String.format("SELECT %s FROM (%s) baseQuery",
                targetColumns,
                removeSqlJunk(baseQuery));

        CSVParser csvParser = query(sql);

        while (rawIterator.hasNext()) {
            try {
                CSVRecord record = rawIterator.next();
                Datum curRow = parseRecord(record);
                dataStream.add(curRow);
                numRows++;
            } catch (NumberFormatException e) {
                badRows++;
            }
        }

    }
}

