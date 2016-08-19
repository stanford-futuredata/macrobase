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
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
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
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BigDAWGIngester extends DataIngester {
    private static final Logger log = LoggerFactory.getLogger(BigDAWGIngester.class);

    private final String dbUrl;
    private final Integer timeColumn;
    protected final String baseQuery;
    private static final String LIMIT_REGEX = "(LIMIT\\s\\d+)";

    @Override
    public String getBaseQuery() {
        return null;
    }

    public BigDAWGIngester(MacroBaseConf conf) throws ConfigurationException, SQLException {
        super(conf);

        baseQuery = conf.getString(MacroBaseConf.BASE_QUERY);
        dbUrl = conf.getString(MacroBaseConf.DB_URL, MacroBaseDefaults.DB_URL);
        timeColumn = conf.getInt(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);

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

    private String removeLimit(String sql) {
        return sql.replaceAll(LIMIT_REGEX, "");
    }

    private String removeSqlJunk(String sql) {
        return sql.replaceAll(";", "");
    }

    @Override
    public Schema getSchema(String baseQuery) throws Exception {
        String sql = String.format("%s LIMIT 1", removeSqlJunk(removeLimit(baseQuery)));

        CSVParser csvParser = query(sql);
        List<Schema.SchemaColumn> columns = Lists.newArrayList();
        for (String name : csvParser.getHeaderMap().keySet()) {
            // TODO: Figure out how to get schema information from BigDAWG
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
        for (Map.Entry<String, Integer> entry : schema.entrySet()) {
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
            for (int i = 0; i < schema.size(); i++) {
                columnValues.add(
                        new ColumnValue(reverseSchema.get(i), record.get(i))
                );
            }
            rows.add(new RowSet.Row(columnValues));
        }

        return new RowSet(rows);
    }

    private Datum parseRecord(CSVRecord record, Map<String, Integer> headerMap) throws NumberFormatException {
        int vecPos = 0;

        RealVector metricVec = new ArrayRealVector(lowMetrics.size() + highMetrics.size());
        for (String metric : lowMetrics) {
            double val = Math.pow(Math.max(Double.parseDouble(record.get(metric)), 0.1), -1);
            metricVec.setEntry(vecPos, val);
            vecPos += 1;
        }

        for (String metric : highMetrics) {
            if (record.get(metric).equals("null")) {
                metricVec.setEntry(vecPos, Double.NEGATIVE_INFINITY);
                log.debug("found null!");
            } else {
                metricVec.setEntry(vecPos, Double.parseDouble(record.get(metric)));
            }
            vecPos += 1;
        }

        List<Integer> attrList = new ArrayList<>(attributes.size());
        for (String attr : attributes) {
            int pos = headerMap.get(attr);
            attrList.add(conf.getEncoder().getIntegerEncoding(pos + 1, record.get(pos)));
        }

        List<Integer> contextualDiscreteAttributesValues = null;
        if (!contextualDiscreteAttributes.isEmpty()) {
            contextualDiscreteAttributesValues = new ArrayList<>(contextualDiscreteAttributes.size());

            for (String attr : contextualDiscreteAttributes) {
                int pos = headerMap.get(attr);
                contextualDiscreteAttributesValues.add(conf.getEncoder().getIntegerEncoding(pos + 1, record.get(pos)));
            }
        }

        RealVector contextualDoubleAttributesValues = null;

        if (!contextualDoubleAttributes.isEmpty()) {
            contextualDoubleAttributesValues = new ArrayRealVector(contextualDoubleAttributes.size());
            vecPos = 0;
            for (String attr : contextualDoubleAttributes) {
                contextualDoubleAttributesValues.setEntry(vecPos, Double.parseDouble(record.get(attr)));
                vecPos += 1;
            }
        }

        return new Datum(
                attrList,
                metricVec,
                contextualDiscreteAttributesValues,
                contextualDoubleAttributesValues
        );
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

        // Load all records into memory to filter out rows with missing data
        Iterator<CSVRecord> rawIterator = csvParser.iterator();

        MBStream<Datum> dataStream = new MBStream<>();

        int badRows = 0;
        int numRows = 0;
        while (rawIterator.hasNext()) {
            try {
                CSVRecord record = rawIterator.next();
                Datum curRow = parseRecord(record, csvParser.getHeaderMap());
                dataStream.add(curRow);
                numRows++;
            } catch (NumberFormatException e) {
                log.debug("error: {}", e);
                badRows++;
            }
        }
        log.info("{}/{} bad rows", badRows, numRows);
        return dataStream;
    }
}

