package macrobase.ingest;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
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

    private ArrayList<Datum> cachedData;
    private Iterator<Datum> cachedIterator;
    private int badRows = 0;

    public enum Compression {
        UNCOMPRESSED,
        GZIP
    }

    public CSVIngester(MacroBaseConf conf) throws ConfigurationException, IOException {
        super(conf);

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

        for (Map.Entry<String, Integer> se : schema.entrySet()) {
            conf.getEncoder().recordAttributeName(se.getValue() + 1, se.getKey());
        }

        // Load all records into memory to filter out rows with missing data
        Iterator<CSVRecord> rawIterator = csvParser.iterator();
        cachedData = new ArrayList<>(1000);

        while (rawIterator.hasNext()) {
            try {
                CSVRecord record = rawIterator.next();
                Datum curRow = parseRecord(record);
                cachedData.add(curRow);
            } catch (NumberFormatException e) {
                badRows++;
            }
        }
        log.info("{}/{} bad rows", badRows, cachedData.size());

        cachedIterator = cachedData.iterator();
    }

    @Override
    public String getBaseQuery() {
        return filename;
    }

    public Datum next() {
        return cachedIterator.next();
    }

    public boolean hasNext() {
        return cachedIterator.hasNext();
    }

    private Datum parseRecord(CSVRecord record) throws NumberFormatException {
        int vecPos = 0;

        RealVector metricVec = new ArrayRealVector(lowMetrics.size() + highMetrics.size());
        for (String metric : lowMetrics) {
            double val = Math.pow(Math.max(Double.parseDouble(record.get(metric)), 0.1), -1);
            metricVec.setEntry(vecPos, val);
            vecPos += 1;
        }

        for (String metric : highMetrics) {
            metricVec.setEntry(vecPos, Double.parseDouble(record.get(metric)));
            vecPos += 1;
        }

        List<Integer> attrList = new ArrayList<>(attributes.size());
        for (String attr : attributes) {
            int pos = schema.get(attr);
            attrList.add(conf.getEncoder().getIntegerEncoding(pos + 1, record.get(pos)));
        }

        List<Integer> contextualDiscreteAttributesValues = null;
        if(!contextualDiscreteAttributes.isEmpty()) {
            contextualDiscreteAttributesValues = new ArrayList<>(contextualDiscreteAttributes.size());

            for (String attr : contextualDiscreteAttributes) {
                int pos = schema.get(attr);
                contextualDiscreteAttributesValues.add(conf.getEncoder().getIntegerEncoding(pos + 1, record.get(pos)));
            }
        }

        RealVector contextualDoubleAttributesValues = null;

        if(!contextualDoubleAttributes.isEmpty()) {
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

    public Schema getSchema(String baseQuery) {
        return null;
    }
}

