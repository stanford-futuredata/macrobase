package macrobase.ingest;

import com.google.common.collect.Lists;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import macrobase.runtime.resources.RowSetResource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

public class CsvLoader extends DataLoader {

    private CSVParser csvParser;
    private Map<String, Integer> schema;

    @Override
    public Schema getSchema(String baseQuery) {
        return null;
    }

    @Override
    public List<Datum> getData(DatumEncoder encoder, List<String> attributes, List<String> lowMetrics, List<String> highMetrics, String baseQuery) throws IOException {
        List<Datum> ret = Lists.newArrayList();
        System.out.println(attributes);
        System.out.println(lowMetrics);
        System.out.println(highMetrics);
        System.out.println(baseQuery);
        for (CSVRecord record : csvParser) {
            RealVector metricVec = new ArrayRealVector(lowMetrics.size() + highMetrics.size());
            // TODO: implemnt lowMetrics
            int vecPos = 0;
            for (String metric : highMetrics) {
                metricVec.setEntry(vecPos, Double.parseDouble(record.get(metric)));
                vecPos += 1;
            }
            List<Integer> attrList = new ArrayList<>(attributes.size());

            // I have no idea why is this code here...
            int i = 1;
            for(String attr : attributes) {
                attrList.add(encoder.getIntegerEncoding(i, attr));
                i += 1;
            }
            ret.add(new Datum(attrList, metricVec));
        }
        return ret;
    }

    @Override
    public RowSet getRows(String baseQuery, List<RowSetResource.RowSetRequest.RowRequestPair> preds, int limit, int offset) throws IOException {
        return null;
    }

    @Override
    public void connect(String source) throws IOException {
        File csvFile = new File(source);
        csvParser = CSVParser.parse(csvFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
        schema = csvParser.getHeaderMap();
    }

    @Override
    public void setDatabaseCredentials(String user, String password) {

    }
}
