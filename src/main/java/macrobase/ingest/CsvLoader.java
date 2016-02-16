package macrobase.ingest;

import com.google.common.collect.Lists;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import macrobase.ingest.transform.DataTransformation;
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
    public CsvLoader(MacroBaseConf conf) throws ConfigurationException, IOException {
        super(conf);

        File csvFile = new File(conf.getString(MacroBaseConf.CSV_INPUT_FILE));
        csvParser = CSVParser.parse(csvFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
        schema = csvParser.getHeaderMap();
    }

    private CSVParser csvParser;
    private Map<String, Integer> schema;

    @Override
    public Schema getSchema(String baseQuery) {
        return null;
    }

    @Override
    public List<Datum> getData(DatumEncoder encoder) {
        for(Map.Entry<String, Integer> se : schema.entrySet()) {
            encoder.recordAttributeName(se.getValue()+1, se.getKey());
        }

        List<Datum> ret = Lists.newArrayList();
        for (CSVRecord record : csvParser) {
            RealVector metricVec = new ArrayRealVector(lowMetrics.size() + highMetrics.size());
            int vecPos = 0;

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

            int i = 1;
            for(String attr : attributes) {
                attrList.add(encoder.getIntegerEncoding(i, record.get(schema.get(attr))));
                i += 1;
            }
            ret.add(new Datum(attrList, metricVec));
        }

        // normalize data
        if (dataTransformation != null ) {
            dataTransformation.transform(ret);
        }

        return ret;
    }

    @Override
    public RowSet getRows(String baseQuery, List<RowSetResource.RowSetRequest.RowRequestPair> preds, int limit, int offset) throws IOException {
        return null;
    }
}
