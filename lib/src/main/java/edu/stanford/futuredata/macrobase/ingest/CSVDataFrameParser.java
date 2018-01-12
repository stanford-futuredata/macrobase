package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import com.univocity.parsers.csv.CsvParser;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;

public class CSVDataFrameParser implements DataFrameLoader {
    private CsvParser parser;
    private List<String> requiredColumns = null;
    private Map<String, Schema.ColType> columnTypes;
    private int numBadRecords = 0;

    public CSVDataFrameParser(CsvParser parser, List<String> requiredColumns) {
        this.parser = parser;
        this.requiredColumns = requiredColumns;
    }
    public CSVDataFrameParser(CsvParser parser) {
        this.parser = parser;
    }

    @Override
    public DataFrameLoader setColumnTypes(Map<String, Schema.ColType> types) {
        this.columnTypes = types;
        return this;
    }

    @Override
    public DataFrame load() throws Exception {
        if (requiredColumns != null) {
            System.out.println(Arrays.toString(requiredColumns.toArray()));
        }
        String[] header = parser.parseNext();
        Map<String, Integer> headerMap = new HashMap<>();
        for (int i = 0; i < header.length; i++) {
            headerMap.put(header[i], i);
        }
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

        this.numBadRecords = 0;
        ArrayList<Row> rows = new ArrayList<>();
        String[] row;
        while ((row = parser.parseNext()) != null) {
            ArrayList<Object> rowFields = new ArrayList<>(numColumns);
            for (int c = 0; c < numColumns; c++) {
                Schema.ColType t = columnTypeList[c];
                String rowValue = row[c];
                if (t == Schema.ColType.STRING) {
                    rowFields.add(rowValue);
                } else if (t == Schema.ColType.DOUBLE) {
                    try {
                        rowFields.add(Double.parseDouble(rowValue));
                    } catch (NumberFormatException e) {
                        rowFields.add(Double.NaN);
                    }
                } else {
                    throw new RuntimeException("Bad ColType");
                }
            }
            rows.add(new Row(rowFields));
        }

        DataFrame df = new DataFrame(schema, rows);
        return df;
    }

    public int getNumBadRecords() {
        return numBadRecords;
    }
}
