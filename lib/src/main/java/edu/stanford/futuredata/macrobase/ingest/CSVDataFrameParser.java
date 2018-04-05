package edu.stanford.futuredata.macrobase.ingest;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVDataFrameParser implements DataFrameLoader {
    private Logger log = LoggerFactory.getLogger(CSVDataFrameParser.class);
    private CsvParser parser;
    private final List<String> requiredColumns;
    private Map<String, Schema.ColType> columnTypes;

    public CSVDataFrameParser(CsvParser parser, List<String> requiredColumns) {
        this.requiredColumns = requiredColumns;
        this.parser = parser;
    }

    public CSVDataFrameParser(String filename, List<String> requiredColumns) throws IOException {
        this.requiredColumns = requiredColumns;
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        settings.setMaxCharsPerColumn(16384);
        CsvParser csvParser = new CsvParser(settings);
        csvParser.beginParsing(getReader(filename));
        this.parser = csvParser;
    }

    public CSVDataFrameParser(String filename, Map<String, Schema.ColType> types) throws IOException {
        this.requiredColumns = new ArrayList<>(types.keySet());
        this.columnTypes = types;
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        CsvParser csvParser = new CsvParser(settings);
        csvParser.beginParsing(getReader(filename));
        this.parser = csvParser;
    }

    @Override
    public DataFrameLoader setColumnTypes(Map<String, Schema.ColType> types) {
        this.columnTypes = types;
        return this;
    }

    @Override
    public DataFrame load() throws Exception {
        String[] header = parser.parseNext();

        int numColumns = header.length;
        int schemaLength = requiredColumns.size();
        int schemaIndexMap[] = new int[numColumns];
        Arrays.fill(schemaIndexMap, -1);

        String[] columnNameList = new String[schemaLength];
        Schema.ColType[] columnTypeList = new Schema.ColType[schemaLength];
        for (int c = 0, schemaIndex = 0; c < numColumns; c++) {
            String columnName = header[c];
            Schema.ColType t = columnTypes.getOrDefault(columnName, Schema.ColType.STRING);
            if (requiredColumns.contains(columnName)) {
                columnNameList[schemaIndex] = columnName;
                columnTypeList[schemaIndex] = t;
                schemaIndexMap[c] = schemaIndex;
                schemaIndex++;
            }
        }
        // Make sure to generate the schema in the right order
        Schema schema = new Schema();
        int numStringColumns = 0;
        int numDoubleColumns = 0;
        for (int c = 0; c < schemaLength; c++) {
            schema.addColumn(columnTypeList[c], columnNameList[c]);
            if (columnTypeList[c] == Schema.ColType.STRING) {
                numStringColumns++;
            } else if (columnTypeList[c] == Schema.ColType.DOUBLE) {
                numDoubleColumns++;
            } else {
                throw new RuntimeException("Bad ColType");
            }
        }

        ArrayList<String>[] stringColumns = (ArrayList<String>[])new ArrayList[numStringColumns];
        for (int i = 0; i < numStringColumns; i++) {
            stringColumns[i] = new ArrayList<>();
        }
        ArrayList<Double>[] doubleColumns = (ArrayList<Double>[])new ArrayList[numDoubleColumns];
        for (int i = 0; i < numDoubleColumns; i++) {
            doubleColumns[i] = new ArrayList<>();
        }

        String[] row;
        int doubleParseFailures = 0;
        while ((row = parser.parseNext()) != null) {
            for (int c = 0, stringColNum = 0, doubleColNum = 0; c < numColumns; c++) {
                if (schemaIndexMap[c] >= 0) {
                    int schemaIndex = schemaIndexMap[c];
                    Schema.ColType t = columnTypeList[schemaIndex];
                    String rowValue = row[c];
                    if (t == Schema.ColType.STRING) {
                        stringColumns[stringColNum++].add(rowValue);
                    } else if (t == Schema.ColType.DOUBLE) {
                        try {
                            doubleColumns[doubleColNum].add(Double.parseDouble(rowValue));
                        } catch (NumberFormatException | NullPointerException e) {
                            doubleColumns[doubleColNum].add(Double.NaN);
                            doubleParseFailures++;
                        }
                        doubleColNum++;
                    } else {
                        throw new RuntimeException("Bad ColType");
                    }
                }
            }
        }
        if (doubleParseFailures > 0)
            log.warn("{} double values failed to parse", doubleParseFailures);

        DataFrame df = new DataFrame(schema, stringColumns, doubleColumns);
        return df;
    }

    private static Reader getReader(String path) {
        try {
            InputStream targetStream = new FileInputStream(
                path.replaceFirst("^~", System.getProperty("user.home")));
            return new InputStreamReader(targetStream, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("File " + path + "is not encoded using UTF-8", e);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("File " + path + " cannot be found", e);
        }
    }
}
