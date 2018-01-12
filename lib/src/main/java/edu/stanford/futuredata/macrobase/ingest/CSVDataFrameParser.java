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
        String[] header = parser.parseNext();

        int numColumns = header.length;
        int schemaLength = header.length;
        if (requiredColumns != null) {
            schemaLength = requiredColumns.size();
        }
        int schemaIndexMap[] = new int[numColumns];
        Arrays.fill(schemaIndexMap, -1);

        String[] columnNameList = new String[schemaLength];
        Schema.ColType[] columnTypeList = new Schema.ColType[schemaLength];
        for (int c = 0, schemaIndex = 0; c < numColumns; c++) {
            String columnName = header[c];
            Schema.ColType t = columnTypes.getOrDefault(columnName, Schema.ColType.STRING);
            if (requiredColumns == null || requiredColumns.contains(columnName)) {
                columnNameList[schemaIndex] = columnName;
                columnTypeList[schemaIndex] = t;
                schemaIndexMap[c] = schemaIndex;
                schemaIndex++;
            }
        }
        // Make sure to generate the schema in the right order
        Schema schema = new Schema();
        for (int c = 0; c < schemaLength; c++) {
            schema.addColumn(columnTypeList[c], columnNameList[c]);
        }

        this.numBadRecords = 0;
        ArrayList<Row> rows = new ArrayList<>();
        String[] row;
        while ((row = parser.parseNext()) != null) {
            ArrayList<Object> rowFields = new ArrayList<>(schemaLength);
            for (int c = 0; c < numColumns; c++) {
                if (schemaIndexMap[c] >= 0) {
                    int schemaIndex = schemaIndexMap[c];
                    Schema.ColType t = columnTypeList[schemaIndex];
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
