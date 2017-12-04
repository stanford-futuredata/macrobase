package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.util.ArrayList;
import java.util.Map;

public class CSVDataFrameParser implements DataFrameLoader {
    private CSVParser parser;
    private Map<String, Schema.ColType> columnTypes;
    private int numBadRecords = 0;

    public CSVDataFrameParser(CSVParser parser) {
        this.parser = parser;
    }

    // TODO: merge this into the constructor; no need for it to be separate
    @Override
    public DataFrameLoader setColumnTypes(Map<String, Schema.ColType> types) {
        this.columnTypes = types;
        return this;
    }

    @Override
    public DataFrame load() throws Exception {
        Map<String, Integer> headerMap = parser.getHeaderMap();
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
        for (CSVRecord record : parser) {
            ArrayList<Object> rowFields = new ArrayList<>(numColumns);
            for (int c = 0; c < numColumns; c++) {
                Schema.ColType t = columnTypeList[c];
                String rowValue = record.get(c);
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
