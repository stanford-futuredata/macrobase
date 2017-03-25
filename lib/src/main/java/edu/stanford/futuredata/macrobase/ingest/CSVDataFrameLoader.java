package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CSVDataFrameLoader implements DataFrameLoader {
    private String fileName;
    private Map<String, Schema.ColType> columnTypes;
    private int badRecords;

    public CSVDataFrameLoader(String fileName){
        this.fileName = fileName;
        this.columnTypes = new HashMap<>();
    }
    @Override
    public DataFrameLoader setColumnTypes(Map<String, Schema.ColType> types) {
        this.columnTypes = types;
        return this;
    }

    @Override
    public DataFrame load() throws Exception {
        File csvFile = new File(fileName);
        CSVParser csvParser = CSVParser.parse(
                csvFile,
                Charset.defaultCharset(),
                CSVFormat.DEFAULT.withHeader()
        );

        Map<String, Integer> headerMap = csvParser.getHeaderMap();
        int numColumns = headerMap.size();

        String[] columnNameList = new String[numColumns];
        Schema.ColType[] columnTypeList = new Schema.ColType[numColumns];
        for (String columnName: headerMap.keySet()) {
            int columnIndex = headerMap.get(columnName);
            Schema.ColType t = columnTypes.getOrDefault(columnName, Schema.ColType.STRING);
            columnNameList[columnIndex] = columnName;
            columnTypeList[columnIndex] = t;
        }
        // Make sure to generate the schema in the right order
        Schema schema = new Schema();
        for (int c = 0; c < numColumns; c++) {
            schema.addColumn(columnTypeList[c], columnNameList[c]);
        }

        this.badRecords = 0;
        ArrayList<Row> rows = new ArrayList<>();
        for (CSVRecord record : csvParser) {
            try {
                ArrayList<Object> rowFields = new ArrayList<>(numColumns);
                for (int c = 0; c < numColumns; c++) {
                    Schema.ColType t = columnTypeList[c];
                    String rowValue = record.get(c);
                    if (t == Schema.ColType.STRING) {
                        rowFields.add(rowValue);
                    } else if (t == Schema.ColType.DOUBLE) {
                        rowFields.add(Double.parseDouble(rowValue));
                    } else {
                        throw new RuntimeException("Bad ColType");
                    }
                }
                rows.add(new Row(rowFields));
            } catch (NumberFormatException e) {
                this.badRecords++;
            }
        }

        DataFrame df = new DataFrame(schema, rows);
        return df;
    }

    public int getBadRecords() {
        return badRecords;
    }
}
