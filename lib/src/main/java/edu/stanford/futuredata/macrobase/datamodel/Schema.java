package edu.stanford.futuredata.macrobase.datamodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
    public Schema copy() {
        Schema other = new Schema();
        other.columnNames = new ArrayList<>(columnNames);
        other.columnTypes = new ArrayList<>(columnTypes);
        other.columnIndices = new HashMap<>(columnIndices);
        return other;
    }

    public String toString() {
        int d = columnNames.size();
        List<String> pairs = new ArrayList<>(d);
        for (int i = 0; i < d; i++) {
            pairs.add(columnNames.get(i)+":"+columnTypes.get(i));
        }
        return pairs.toString();
    }

    public boolean hasColumn(String columnName) { return columnNames.contains(columnName); }
    public int getNumColumns() {
        return columnNames.size();
    }
    public int getColumnIndex(String s) {
        return columnIndices.get(s);
    }
    public ArrayList<Integer> getColumnIndices(List<String> columns) {
        ArrayList<Integer> indices = new ArrayList<>(columns.size());
        for (String colName: columns) {
            indices.add(getColumnIndex(colName));
        }
        return indices;
    }
    public String getColumnName(int i) {
        return columnNames.get(i);
    }
    public List<String> getColumnNames() {
        return this.columnNames;
    }
    public List<String> getColumnNamesByType(ColType type) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i ++) {
            if (getColumnType(i).equals(type)) {
                names.add(getColumnName(i));
            }
        }
        return names;
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
