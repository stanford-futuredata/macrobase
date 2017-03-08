package macrobase.datamodel;

import java.util.*;

/**
 * Provides column names, types, and order
 */
public class Schema {
    public enum ColType {
        STRING,
        DOUBLE
    }
    private ArrayList<String> columnNames;
    private ArrayList<ColType> columnTypes;
    private HashMap<String, Integer> columnIndices;

    public Schema() {
        this.columnNames = new ArrayList<>();
        this.columnTypes = new ArrayList<>();
        this.columnIndices = new HashMap<>();
    }

    public int getNumColumns() {
        return columnNames.size();
    }
    public int getColumnIndex(String s) {
        return columnIndices.get(s);
    }
    public String getColumnName(int i) {
        return columnNames.get(i);
    }
    public ColType getColumnType(int i) {
        return columnTypes.get(i);
    }
    public ColType getColumnTypeByName(String s) {
        return getColumnType(getColumnIndex(s));
    }

    public Schema addColumn(ColType t, String colName) {
        int nextIdx = columnNames.size();
        this.columnNames.add(colName);
        this.columnTypes.add(t);
        this.columnIndices.put(colName, nextIdx);
        return this;
    }
}
