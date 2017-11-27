package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CSVDataFrameLoader implements DataFrameLoader {
    private String fileName;
    private CSVDataFrameParser parserWrapper;

    public CSVDataFrameLoader(String fileName) throws IOException {
        this.fileName = fileName;
        File csvFile = new File(fileName);
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

    public int getBadRecords() {
        return parserWrapper.getNumBadRecords();
    }
}
