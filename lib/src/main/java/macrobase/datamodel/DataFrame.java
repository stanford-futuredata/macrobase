package macrobase.datamodel;

import macrobase.conf.MacrobaseException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoublePredicate;
import java.util.function.Predicate;

import static macrobase.datamodel.Schema.ColType;

/**
 * Column-based dataframe object.
 * loadRows and addColumn methods mutate the dataframe and are the primary
 * ways of initializing the data in the dataframe.
 */
public class DataFrame {
    private Schema schema;

    private ArrayList<String[]> stringCols;
    private ArrayList<double[]> doubleCols;
    private ArrayList<Integer> indexToTypeIndex = new ArrayList<>();

    private int numRows;

    public DataFrame() {
        this.schema = new Schema();
        this.stringCols = new ArrayList<>();
        this.doubleCols = new ArrayList<>();
        this.indexToTypeIndex = new ArrayList<>();
        this.numRows = 0;
    }

    /**
     * @return shallow copy of dataframe
     */
    public DataFrame copy() {
        DataFrame other = new DataFrame();
        other.schema = schema.copy();
        other.stringCols = new ArrayList<>(stringCols);
        other.doubleCols = new ArrayList<>(doubleCols);
        other.indexToTypeIndex = new ArrayList<>(indexToTypeIndex);
        other.numRows = numRows;
        return other;
    }

    public Schema getSchema() {return this.schema;}
    public int getNumRows() {return numRows;}
    public ArrayList<double[]> getDoubleCols() { return doubleCols; }
    public ArrayList<String[]> getStringCols() { return stringCols; }

    // Fast Column-based methods
    private void addDoubleColumnInternal(double[] colValues) {
        doubleCols.add(colValues);
        indexToTypeIndex.add(doubleCols.size()-1);
    }
    public DataFrame addDoubleColumn(String colName, double[] colValues) {
        if (numRows == 0) {
            numRows = colValues.length;
        }
        schema.addColumn(ColType.DOUBLE, colName);
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
        schema.addColumn(ColType.STRING, colName);
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
     * @param columns column indices to project
     * @return new dataframe with subset of columns
     */
    public DataFrame select(List<Integer> columns) throws MacrobaseException {
        DataFrame other = new DataFrame();
        for (int c : columns) {
            String columnName = schema.getColumnName(c);
            ColType t = schema.getColumnType(c);
            if (t == ColType.STRING) {
                other.addStringColumn(columnName, getStringColumn(c));
            } else if (t == ColType.DOUBLE) {
                other.addDoubleColumn(columnName, getDoubleColumn(c));
            } else {
                throw new MacrobaseException("Bad Column Type");
            }
        }
        return other;
    }

    /**
     * @param columns column names to project
     * @return new dataframe with subset of columns
     */
    public DataFrame selectByName(List<String> columns) throws MacrobaseException {
        return select(this.schema.getColumnIndices(columns));
    }

    /**
     * @param mask rows to select
     * @return new dataframe with subset of rows
     */
    public DataFrame filter(boolean[] mask) throws MacrobaseException {
        DataFrame other = new DataFrame();

        int d = schema.getNumColumns();
        int numTrue = 0;
        for (int i = 0; i < numRows; i++) {
            if (mask[i]) {
                numTrue++;
            }
        }
        for (int c = 0; c < d; c++) {
            ColType t = schema.getColumnType(c);
            String columnName = schema.getColumnName(c);
            if (t == ColType.STRING) {
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
            } else if (t == ColType.DOUBLE) {
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
                throw new MacrobaseException("Bad Column Type");
            }
        }
        return other;
    }
    public DataFrame filter(int columnIdx, Predicate<String> filter) throws MacrobaseException {
        String[] filterColumn = getStringColumn(columnIdx);
        boolean[] mask = new boolean[numRows];
        for (int i = 0; i < numRows; i++) {
            mask[i] = filter.test(filterColumn[i]);
        }
        return filter(mask);
    }
    public DataFrame filter(String columnName, Predicate<String> filter) throws MacrobaseException{
        return filter(schema.getColumnIndex(columnName), filter);
    }

    /**
     * @param columnIdx column index to filter by
     * @param filter predicate to test each column value
     * @return new dataframe with subset of rows
     */
    public DataFrame filter(int columnIdx, DoublePredicate filter) throws MacrobaseException {
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
    public DataFrame filter(String columnName, DoublePredicate filter) throws MacrobaseException {
        return filter(schema.getColumnIndex(columnName), filter);
    }

    /**
     * Overwrites existing dataframe contents with new batch of rows.
     * Prefer loading data by column via addXColumn methods if possible.
     * @param schema new schema, overrides existing schema
     * @param rows list of untyped rows
     * @return dataframe updated with new content
     */
    public DataFrame loadRows(Schema schema, List<Row> rows) throws MacrobaseException {
        this.schema = schema;
        this.numRows = rows.size();
        int d = schema.getNumColumns();
        for (int c = 0; c < d; c++) {
            ColType t = schema.getColumnType(c);
            if (t == ColType.STRING) {
                String[] colValues = new String[numRows];
                for (int i = 0; i < numRows; i++) {
                    colValues[i] = rows.get(i).<String>getAs(c);
                }
                addStringColumnInternal(colValues);
            } else if (t == ColType.DOUBLE) {
                double[] colValues = new double[numRows];
                for (int i = 0; i < numRows; i++) {
                    colValues[i] = rows.get(i).<Double>getAs(c);
                }
                addDoubleColumnInternal(colValues);
            } else {
                throw new MacrobaseException("Invalid ColType");
            }
        }
        return this;
    }

    public Row getRow(int rowIdx) throws MacrobaseException {
        int d = schema.getNumColumns();
        ArrayList<Object> rowValues = new ArrayList<>(d);
        for (int c = 0; c < d; c++) {
            ColType t = schema.getColumnType(c);
            int typeSubIndex = indexToTypeIndex.get(c);
            if (t == ColType.STRING) {
                rowValues.add(stringCols.get(typeSubIndex)[rowIdx]);
            } else if (t == ColType.DOUBLE) {
                rowValues.add(doubleCols.get(typeSubIndex)[rowIdx]);
            } else {
                throw new MacrobaseException("Bad ColType");
            }
        }
        Row r = new Row(schema, rowValues);
        return r;
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
