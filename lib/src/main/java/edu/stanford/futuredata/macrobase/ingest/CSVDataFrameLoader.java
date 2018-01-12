package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.*;
import java.util.Map;
import java.util.List;

public class CSVDataFrameLoader implements DataFrameLoader {
    private String fileName;
    private CSVDataFrameParser parserWrapper;

    public CSVDataFrameLoader(String fileName) throws IOException {
        this.fileName = fileName;
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        CsvParser csvParser = new CsvParser(settings);
        csvParser.beginParsing(getReader(fileName));
        this.parserWrapper = new CSVDataFrameParser(csvParser);
    }

    public CSVDataFrameLoader(String fileName, List<String> requiredColumns) throws IOException {
        this.fileName = fileName;
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        CsvParser csvParser = new CsvParser(settings);
        csvParser.beginParsing(getReader(fileName));
        this.parserWrapper = new CSVDataFrameParser(csvParser, requiredColumns);
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

    private static Reader getReader(String path) {
        try {
            InputStream targetStream = new FileInputStream(path);
            return new InputStreamReader(targetStream, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Unable to read input", e);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Unable to read input", e);
        }
    }

    public int getBadRecords() {
        return parserWrapper.getNumBadRecords();
    }
}
