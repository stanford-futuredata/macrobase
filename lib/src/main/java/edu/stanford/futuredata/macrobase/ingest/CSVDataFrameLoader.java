package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

public class CSVDataFrameLoader implements DataFrameLoader {

    private CSVDataFrameParser parserWrapper;

    public CSVDataFrameLoader(String filename) throws IOException {
        File csvFile = new File(filename);
        CSVParser csvParser = CSVParser.parse(
                csvFile,
                Charset.defaultCharset(),
                CSVFormat.DEFAULT.withHeader()
        );
        this.parserWrapper = new CSVDataFrameParser(csvParser);
    }
    @Override
    public DataFrameLoader setColumnTypes(Map<String, Schema.ColType> types) {
        parserWrapper.setColumnTypes(types);
        return this;
    }

    @Override
    public DataFrame load() throws Exception {
        return parserWrapper.load();
    }
}
