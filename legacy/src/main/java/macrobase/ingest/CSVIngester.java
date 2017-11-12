package macrobase.ingest;

import com.google.common.collect.Lists;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.result.RowSet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class CSVIngester extends DataIngester {
    private static final Logger log = LoggerFactory.getLogger(CSVIngester.class);
    private CSVParser csvParser;
    private Map<String, Integer> schema;
    private String filename;

    private MBStream<Datum> dataStream = new MBStream<>();
    private boolean loaded = false;

    private int badRows = 0;

    public enum Compression {
        UNCOMPRESSED,
        GZIP
    }

    public CSVIngester(MacroBaseConf conf) throws ConfigurationException, IOException {
        super(conf);
    }

    //equal to getNext() in SQL ingest
    private Datum parseRecord(CSVRecord record) throws NumberFormatException {
        int vecPos = 0;

        RealVector metricVec = new ArrayRealVector(metrics.size());
        for (String metric : metrics) {
            metricVec.setEntry(vecPos, Double.parseDouble(record.get(metric)));
            vecPos += 1;
        }

        List<Integer> attrList = new ArrayList<>(attributes.size());
        for (String attr : attributes) {
            int pos = schema.get(attr);
            attrList.add(conf.getEncoder().getIntegerEncoding(pos + 1, record.get(pos)));
        }

        return new Datum(
                attrList,
                metricVec
        );
    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        if(!loaded) {
            long st = System.currentTimeMillis();

            filename = conf.getString(MacroBaseConf.CSV_INPUT_FILE);
            Compression compression = conf.getCsvCompression();

            if (compression == Compression.GZIP) {
                InputStream fileStream = new FileInputStream(filename);
                InputStream gzipStream = new GZIPInputStream(fileStream);
                Reader decoder = new InputStreamReader(gzipStream);
                csvParser = new CSVParser(decoder, CSVFormat.DEFAULT.withHeader());
            } else {
                File csvFile = new File(conf.getString(MacroBaseConf.CSV_INPUT_FILE));
                csvParser = CSVParser.parse(csvFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
            }
            schema = csvParser.getHeaderMap(); //equal to resultSet.getmetadata or smt

            for (Map.Entry<String, Integer> se : schema.entrySet()) {
                conf.getEncoder().recordAttributeName(se.getValue() + 1, se.getKey()); //numbering off each column for encoding
            }

            // Load all records into memory to filter out rows with missing data
            Iterator<CSVRecord> rawIterator = csvParser.iterator();

            int numRows = 0;
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
            log.info("{}/{} rows successfully parsed ({} malformed rows)", numRows, numRows + badRows, badRows);
        }

        return dataStream;
    }

    @Override
    public RowSet getRows(String baseQuery,
                          Map<String, String> preds,
                          int limit,
                          int offset) throws Exception{

        filename = conf.getString(MacroBaseConf.CSV_INPUT_FILE);
        Compression compression = conf.getCsvCompression();

        if (compression == Compression.GZIP) {
            InputStream fileStream = new FileInputStream(filename);
            InputStream gzipStream = new GZIPInputStream(fileStream);
            Reader decoder = new InputStreamReader(gzipStream);
            csvParser = new CSVParser(decoder, CSVFormat.DEFAULT.withHeader());
        } else {
            File csvFile = new File(conf.getString(MacroBaseConf.CSV_INPUT_FILE));
            csvParser = CSVParser.parse(csvFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
        }
        schema = csvParser.getHeaderMap();
        Iterator<CSVRecord> rawIterator = csvParser.iterator();
        int rowCount = 0;

        List<RowSet.Row> rows = Lists.newArrayList();
        while (rawIterator.hasNext() && rowCount < limit) {
            CSVRecord record = rawIterator.next();
            List<ColumnValue> columnValues = Lists.newArrayList();

            if (includeRow(record, preds)) {
                for (Map.Entry<String, Integer> se : schema.entrySet()) {
                    columnValues.add(new ColumnValue(se.getKey(),record.get(se.getValue())));
                }

                rows.add(new RowSet.Row(columnValues));
                rowCount++;
            }
        }
         return new RowSet(rows);
    }

    private boolean includeRow(CSVRecord record, Map<String, String> preds) {
        boolean retRow = true;
        for (Map.Entry<String, String> pred : preds.entrySet()) {
            retRow = (retRow && record.get(pred.getKey()).equals(pred.getValue()));
        }
        return retRow;
    }

    }


