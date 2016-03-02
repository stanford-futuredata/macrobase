package macrobase.ingest;

import com.google.common.collect.Lists;
import macrobase.MacroBase;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import macrobase.ingest.transform.DataTransformation;
import macrobase.runtime.resources.RowSetResource;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipFile;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

public class CsvLoader extends DataLoader {
    public enum Compression {
        UNCOMPRESSED,
        GZIP
    }
    public CsvLoader(MacroBaseConf conf) throws ConfigurationException, IOException {
        super(conf);

        String filename = conf.getString(MacroBaseConf.CSV_INPUT_FILE);
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
    }

    private CSVParser csvParser;
    private Map<String, Integer> schema;

    @Override
    public Schema getSchema(String baseQuery) {
        return null;
    }

    @Override
    public List<Datum> getData(DatumEncoder encoder) {
        for (Map.Entry<String, Integer> se : schema.entrySet()) {
            encoder.recordAttributeName(se.getValue() + 1, se.getKey());
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
            for (String attr : attributes) {
                attrList.add(encoder.getIntegerEncoding(i, record.get(schema.get(attr))));
                i += 1;
            }
            ret.add(new Datum(attrList, metricVec));
        }

        // normalize data
        if (dataTransformation != null) {
            dataTransformation.transform(ret);
        }

        return ret;
    }

    @Override
    public RowSet getRows(String baseQuery, List<RowSetResource.RowSetRequest.RowRequestPair> preds, int limit, int offset) throws IOException {
        return null;
    }

	@Override
	public List<Datum> getData(DatumEncoder encoder, List<String> contextualDiscreteAttributes,
			List<String> contextualDoubleAttributes) throws SQLException, IOException {
		 for (Map.Entry<String, Integer> se : schema.entrySet()) {
	            encoder.recordAttributeName(se.getValue() + 1, se.getKey());
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
	            for (String attr : attributes) {
	                attrList.add(encoder.getIntegerEncoding(i, record.get(schema.get(attr))));
	                i += 1;
	            }
	            
	            
	            List<Integer> contextualDiscreteAttributesValues = new ArrayList<>(contextualDiscreteAttributes.size());
	            for (String attr: contextualDiscreteAttributes) {
	            	contextualDiscreteAttributesValues.add(encoder.getIntegerEncoding(i, record.get(schema.get(attr))));
	            	i+= 1;
	            }
	            RealVector contextualDoubleAttributesValues = new ArrayRealVector(contextualDoubleAttributes.size());
	            vecPos = 0;
	            for(String attr: contextualDoubleAttributes){
	            	contextualDoubleAttributesValues.setEntry(vecPos, Double.parseDouble(record.get(attr)));
	            	vecPos += 1;
	            }
	            
	            ret.add(new Datum(attrList, metricVec,contextualDiscreteAttributesValues,contextualDoubleAttributesValues));
	        }

	        // normalize data
	        if (dataTransformation != null) {
	            dataTransformation.transform(ret);
	        }

	        return ret;
	}
}
