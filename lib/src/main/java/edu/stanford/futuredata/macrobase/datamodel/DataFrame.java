package edu.stanford.futuredata.macrobase.datamodel;

import edu.stanford.futuredata.macrobase.util.MacrobaseInternalError;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoublePredicate;
import java.util.function.Predicate;

/**
 * Column-based dataframe object.
 * loadRows and addColumn methods mutate the dataframe and are the primary
 * ways of initializing the data in the dataframe.
 */
public class DataFrame {
    private Schema schema;

    private ArrayList<String[]> stringCols;
    private ArrayList<double[]> doubleCols;
    private ArrayList<Integer> indexToTypeIndex;

    private int numRows;

    public DataFrame() {
        this.schema = new Schema();
        this.stringCols = new ArrayList<>();
        this.doubleCols = new ArrayList<>();
        this.indexToTypeIndex = new ArrayList<>();
        this.numRows = 0;
    }

    /**
     * Creates a dataframe from a list of rows
     * Slower than creating a dataframe column by column using addXColumn methods.
     * @param schema Schema to use
     * @param rows Data to load
     */
    public DataFrame(Schema schema, List<Row> rows) {
        this();
        this.schema = schema;
        this.numRows = rows.size();
        int d = schema.getNumColumns();
        for (int c = 0; c < d; c++) {
            Schema.ColType t = schema.getColumnType(c);
            if (t == Schema.ColType.STRING) {
                String[] colValues = new String[numRows];
                for (int i = 0; i < numRows; i++) {
                    colValues[i] = rows.get(i).<String>getAs(c);
                }
                addStringColumnInternal(colValues);
            } else if (t == Schema.ColType.DOUBLE) {
                double[] colValues = new double[numRows];
                for (int i = 0; i < numRows; i++) {
                    colValues[i] = rows.get(i).<Double>getAs(c);
                }
                addDoubleColumnInternal(colValues);
            } else {
                throw new MacrobaseInternalError("Invalid ColType");
            }
        }
    }

    /**
     * @return shallow copy of dataframe
     */
    public DataFrame copy() {
        DataFrame other = new DataFrame();
        other.schema = schema.copy();
        other.indexToTypeIndex = new ArrayList<>(indexToTypeIndex);
        other.numRows = numRows;
        other.stringCols = new ArrayList<>(stringCols);
        other.doubleCols = new ArrayList<>(doubleCols);
        return other;
    }

    public Schema getSchema() {return this.schema;}
    public int getNumRows() {return numRows;}
    public ArrayList<double[]> getDoubleCols() { return doubleCols; }
    public ArrayList<String[]> getStringCols() { return stringCols; }

    public String toString() {
        return getRows().toString();
    }

    // Fast Column-based methods
    private void addDoubleColumnInternal(double[] colValues) {
        doubleCols.add(colValues);
        indexToTypeIndex.add(doubleCols.size()-1);
    }
    public DataFrame addDoubleColumn(String colName, double[] colValues) {
        if (numRows == 0) {
            numRows = colValues.length;
        }
        schema.addColumn(Schema.ColType.DOUBLE, colName);
        addDoubleColumnInternal(colValues);
        return this;
    }
    private void addStringColumnInternal(String[] colValues) {
        stringCols.add(colValues);
        indexToTypeIndex.add(stringCols.size()-1);
    }
    public DataFrame addStringColumn(String colName, String[] colValues) {
        if (numRows == 0) {
            numRows = colValues.length;
        }
        schema.addColumn(Schema.ColType.STRING, colName);
        addStringColumnInternal(colValues);
        return this;
    }

    protected int[] getSubIndices(List<Integer> columns) {
        int d = columns.size();
        int[] typeSubIndices = new int[d];
        for (int i = 0; i < d; i++) {
            typeSubIndices[i] = indexToTypeIndex.get(columns.get(i));
        }
        return typeSubIndices;
    }

    public double[] getDoubleColumn(int columnIdx) {
        return doubleCols.get(indexToTypeIndex.get(columnIdx));
    }
    public double[] getDoubleColumnByName(String columnName) {
        return doubleCols.get(indexToTypeIndex.get(schema.getColumnIndex(columnName)));
    }
    public ArrayList<double[]> getDoubleCols(List<Integer> columns) {
        ArrayList<double[]> cols = new ArrayList<>();
        for (int c : columns) {
            cols.add(getDoubleColumn(c));
        }
        return cols;
    }
    public ArrayList<double[]> getDoubleColsByName(List<String> columns) {
        return getDoubleCols(this.schema.getColumnIndices(columns));
    }
    public String[] getStringColumn(int columnIdx) {
        return stringCols.get(indexToTypeIndex.get(columnIdx));
    }
    public String[] getStringColumnByName(String columnName) {
        return stringCols.get(indexToTypeIndex.get(schema.getColumnIndex(columnName)));
    }
    public ArrayList<String[]> getStringCols(List<Integer> columns) {
        ArrayList<String[]> cols = new ArrayList<>();
        for (int c : columns) {
            cols.add(getStringColumn(c));
        }
        return cols;
    }
    public ArrayList<String[]> getStringColsByName(List<String> columns) {
        return getStringCols(this.schema.getColumnIndices(columns));
    }

    /**
     * @param others Dataframes to combine
     * @return new dataframe with copied rows
     */
    public static DataFrame unionAll(List<DataFrame> others) {
        int k = others.size();
        if (k == 0) {
            return new DataFrame();
        }

        DataFrame first = others.get(0);
        DataFrame combined = new DataFrame();
        combined.schema = first.schema.copy();
        combined.indexToTypeIndex = new ArrayList<>(first.indexToTypeIndex);
        int n = 0;
        for (int i = 0; i < k; i++) {
            n += others.get(i).numRows;
        }
        combined.numRows = n;

        int d = first.stringCols.size();
        combined.stringCols = new ArrayList<>(d);
        for (int colIdx = 0; colIdx < d; colIdx++) {
            String[] newCol = new String[n];
            int i = 0;
            for (DataFrame curOther : others) {
                String[] otherCol = curOther.getStringColumn(colIdx);
                for (String curString : otherCol) {
                    newCol[i] = curString;
                    i++;
                }
            }
            combined.stringCols.add(newCol);
        }

        d = first.doubleCols.size();
        combined.doubleCols = new ArrayList<>(d);
        for (int colIdx = 0; colIdx < d; colIdx++) {
            double[] newCol = new double[n];
            int i = 0;
            for (DataFrame curOther : others) {
                double[] otherCol = curOther.getDoubleColumn(colIdx);
                for (double curString : otherCol) {
                    newCol[i] = curString;
                    i++;
                }
            }
            combined.doubleCols.add(newCol);
        }

        return combined;
    }

    /**
     * @param columns column indices to project
     * @return new dataframe with subset of columns
     */
    public DataFrame select(List<Integer> columns) {
        DataFrame other = new DataFrame();
        for (int c : columns) {
            String columnName = schema.getColumnName(c);
            Schema.ColType t = schema.getColumnType(c);
            if (t == Schema.ColType.STRING) {
                other.addStringColumn(columnName, getStringColumn(c));
            } else if (t == Schema.ColType.DOUBLE) {
                other.addDoubleColumn(columnName, getDoubleColumn(c));
            } else {
                throw new MacrobaseInternalError("Bad Column Type");
            }
        }
        return other;
    }

    /**
     * @param columns column names to project
     * @return new dataframe with subset of columns
     */
    public DataFrame selectByName(List<String> columns) {
        return select(this.schema.getColumnIndices(columns));
    }

    /**
     * @param mask rows to select
     * @return new dataframe with subset of rows
     */
    protected DataFrame filter(boolean[] mask) {
        DataFrame other = new DataFrame();

        int d = schema.getNumColumns();
        int numTrue = 0;
        for (int i = 0; i < numRows; i++) {
            if (mask[i]) {
                numTrue++;
            }
        }
        for (int c = 0; c < d; c++) {
            Schema.ColType t = schema.getColumnType(c);
            String columnName = schema.getColumnName(c);
            if (t == Schema.ColType.STRING) {
                String[] oldColumn = getStringColumn(c);
                String[] newColumn = new String[numTrue];
                int j = 0;
                for (int i = 0; i < numRows; i++) {
                    if (mask[i]) {
                        newColumn[j] = oldColumn[i];
                        j++;
                    }
                }
                other.addStringColumn(columnName, newColumn);
            } else if (t == Schema.ColType.DOUBLE) {
                double[] oldColumn = getDoubleColumn(c);
                double[] newColumn = new double[numTrue];
                int j = 0;
                for (int i = 0; i < numRows; i++) {
                    if (mask[i]) {
                        newColumn[j] = oldColumn[i];
                        j++;
                    }
                }
                other.addDoubleColumn(columnName, newColumn);
            } else {
                throw new MacrobaseInternalError("Bad Column Type");
            }
        }
        return other;
    }
    public DataFrame filter(int columnIdx, Predicate<Object> filter) {
        String[] filterColumn = getStringColumn(columnIdx);
        boolean[] mask = new boolean[numRows];
        for (int i = 0; i < numRows; i++) {
            mask[i] = filter.test(filterColumn[i]);
        }
        return filter(mask);
    }
    public DataFrame filter(String columnName, Predicate<Object> filter) {
        return filter(schema.getColumnIndex(columnName), filter);
    }

    /**
     * @param columnIdx column index to filter by
     * @param filter predicate to test each column value
     * @return new dataframe with subset of rows
     */
    public DataFrame filter(int columnIdx, DoublePredicate filter) {
        double[] filterColumn = getDoubleColumn(columnIdx);
        boolean[] mask = new boolean[numRows];
        for (int i = 0; i < numRows; i++) {
            mask[i] = filter.test(filterColumn[i]);
        }
        return filter(mask);
    }

    /**
     * @param columnName column name to filter by
     * @param filter predicate to test each column value
     * @return new dataframe with subset of rows
     */
    public DataFrame filter(String columnName, DoublePredicate filter) {
        return filter(schema.getColumnIndex(columnName), filter);
    }

    public Row getRow(int rowIdx) {
        int d = schema.getNumColumns();
        ArrayList<Object> rowValues = new ArrayList<>(d);
        for (int c = 0; c < d; c++) {
            Schema.ColType t = schema.getColumnType(c);
            int typeSubIndex = indexToTypeIndex.get(c);
            if (t == Schema.ColType.STRING) {
                rowValues.add(stringCols.get(typeSubIndex)[rowIdx]);
            } else if (t == Schema.ColType.DOUBLE) {
                rowValues.add(doubleCols.get(typeSubIndex)[rowIdx]);
            } else {
                throw new MacrobaseInternalError("Bad ColType");
            }
        }
        Row r = new Row(schema, rowValues);
        return r;
    }
    public List<Row> getRows() {
        return getRows(numRows);
    }
    public List<Row> getRows(int numRowsToGet) {
        if (numRowsToGet > numRows) {
            numRowsToGet = numRows;
        }
        List<Row> rows = new ArrayList<>();
        for (int rowIdx = 0; rowIdx < numRowsToGet; rowIdx++) {
            rows.add(getRow(rowIdx));
        }
        return rows;
    }

    public ArrayList<double[]> getDoubleRows(List<Integer> columns) {
        ArrayList<double[]> rows = new ArrayList<>(this.numRows);
        int d = columns.size();
        int[] typeSubIndices = getSubIndices(columns);

        for (int i = 0; i < this.numRows; i++) {
            double[] curRow = new double[d];
            for (int j = 0; j < d; j++) {
                int colSubIndex = typeSubIndices[j];
                curRow[j] = doubleCols.get(colSubIndex)[i];
            }
            rows.add(curRow);
        }
        return rows;
    }
    public ArrayList<String[]> getStringRows(List<Integer> columns) {
        ArrayList<String[]> rows = new ArrayList<>(this.numRows);
        int d = columns.size();
        int[] typeSubIndices = getSubIndices(columns);

        for (int i = 0; i < this.numRows; i++) {
            String[] curRow = new String[d];
            for (int j = 0; j < d; j++) {
                int colSubIndex = typeSubIndices[j];
                curRow[j] = stringCols.get(colSubIndex)[i];
            }
            rows.add(curRow);
        }
        return rows;
    }
    public ArrayList<double[]> getDoubleRowsByName(List<String> columns) {
        return getDoubleRows(this.schema.getColumnIndices(columns));
    }
    public ArrayList<String[]> getStringRowsByName(List<String> columns) {
        return getStringRows(this.schema.getColumnIndices(columns));
    }
}
