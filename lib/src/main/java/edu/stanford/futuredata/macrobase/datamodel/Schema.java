package edu.stanford.futuredata.macrobase.datamodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

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

    /**
     * Rename column in schema.
     *
     * @param oldColumnName The name of the column to be renamed. If it doesn't exist, nothing is
     * changed
     * @param newColumnName The new name for the column
     * @return true if rename was successful, false otherwise
     */
    boolean renameColumn(String oldColumnName, String newColumnName) {
        if (!columnIndices.containsKey(oldColumnName)) {
            return false;
        }

        for (int i = 0; i < columnNames.size(); ++i) {
            if (columnNames.get(i).equals(oldColumnName)) {
                columnNames.set(i, newColumnName);
                final int index = columnIndices.remove(oldColumnName);
                columnIndices.put(newColumnName, index);
                return true;
            }
        }
        return false;
    }


    public boolean hasColumn(String columnName) { return columnNames.contains(columnName); }
    public boolean hasColumns(Collection<String> columnNames) {
        return this.columnNames.containsAll(columnNames);
    }

    public int getNumColumns() {
        return columnNames.size();
    }
    public int getColumnIndex(String s) {
        if (!columnIndices.containsKey(s)) {
            throw new UnsupportedOperationException("Column " + s + " not present in the schema");
        }
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        final Schema o = (Schema) obj;
        return Objects.equals(columnTypes, o.columnTypes) &&
            Objects.equals(columnTypes, o.columnTypes) &&
            Objects.equals(columnIndices, o.columnIndices);
    }
}
