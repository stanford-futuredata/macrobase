package edu.stanford.futuredata.macrobase.datamodel;

import static java.util.stream.Collectors.toList;

import com.google.common.base.Joiner;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.util.MacrobaseInternalError;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.DoublePredicate;
import java.util.function.Predicate;
import org.apache.commons.lang3.StringUtils;

/**
 * Column-based DataFrame object.
 * DataFrames are primarily meant for data transfer across operators while
 * preserving column names and types. Complex processing should be done by
 * extracting columns as arrays and operating on arrays directly.
 *
 * The addColumn methods are the primary means of mutating a DataFrame and are
 * especially useful during DataFrame construction. DataFrames can also be
 * initialized from a schema and a set of rows.
 */
public class DataFrame {
    private Schema schema;

    private ArrayList<String[]> stringCols;
    private ArrayList<double[]> doubleCols;
    // external indices define a global ordering on columns, but internally each
    // column is stored with other columns of its type. Thus external indices must be
    // converted into internal type-specific indices.
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
     * Creates a DataFrame from a list of rows
     * Slower than creating a DataFrame column by column using addXColumn methods.
     * @param schema Schema to use
     * @param rows Data to load
     */
    public DataFrame(Schema schema, List<Row> rows) {
        this();
        this.schema = schema;
        this.numRows = rows.size();
        final int numColumns = schema.getNumColumns();
        for (int c = 0; c < numColumns; c++) {
            Schema.ColType t = schema.getColumnType(c);
            if (t == Schema.ColType.STRING) {
                String[] colValues = new String[numRows];
                for (int i = 0; i < numRows; i++) {
                    colValues[i] = rows.get(i).<String>getAs(c);
                }
                stringCols.add(colValues);
                indexToTypeIndex.add(stringCols.size()-1);
            } else if (t == Schema.ColType.DOUBLE) {
                double[] colValues = new double[numRows];
                for (int i = 0; i < numRows; i++) {
                    colValues[i] = rows.get(i).<Double>getAs(c);
                }
                doubleCols.add(colValues);
                indexToTypeIndex.add(doubleCols.size()-1);
            } else {
                throw new MacrobaseInternalError("Invalid ColType");
            }
        }
    }

    public DataFrame(Schema schema, ArrayList<String>[] stringColumns, ArrayList<Double>[] doubleColumns) {
        this();
        this.schema = schema;
        if (stringColumns.length > 0)
            numRows = stringColumns[0].size();
        else
            numRows = doubleColumns[0].size();
        int numColumns = schema.getNumColumns();
        for (int c = 0, stringColNum = 0, doubleColNum = 0; c < numColumns; c++) {
            Schema.ColType t = schema.getColumnType(c);
            if (t == Schema.ColType.STRING) {
                String[] colValues = stringColumns[stringColNum].toArray(new String[numRows]);
                addStringColumnInternal(colValues);
                stringColNum++;
            } else if (t == Schema.ColType.DOUBLE) {
                double[] colValues = new double[numRows];
                for (int i = 0; i < numRows; i++) {
                    colValues[i] = doubleColumns[doubleColNum].get(i).doubleValue();
                }
                addDoubleColumnInternal(colValues);
                doubleColNum++;
            } else {
                throw new MacrobaseInternalError("Invalid ColType");
            }
        }
    }

    /**
     * Shallow copy of the DataFrame: the schema is recreated but the arrays backing the
     * columns are reused.
     * @return shallow DataFrame copy
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

    /**
     * Pretty print contents of the DataFrame to STDOUT. Example output:
     * ------------------------------------------
     * |   col_1  |   col_2  |  ...  |   col_n  |
     * ------------------------------------------
     * |  val_11  |  val_12  |  ...  |  val_1n  |
     * ...
     * |  val_m1  |  val_m2  |  ...  |  val_mn  |
     * ------------------------------------------
     * @param maxNumToPrint maximum number of rows from the DataFrame to print
     */
    public void prettyPrint(final int maxNumToPrint) {
        System.out.println(numRows + " rows");

        final int maxColNameLength = schema.getColumnNames().stream()
            .reduce("", (x, y) -> x.length() > y.length() ? x : y).length() + 2; // extra space on both sides
        final String schemaStr = "|" + Joiner.on("|").join(schema.getColumnNames().stream()
            .map((x) -> StringUtils.center(String.valueOf(x), maxColNameLength)).collect(toList())) + "|";
        final String dashes = Joiner.on("").join(Collections.nCopies(schemaStr.length(), "-"));
        System.out.println(dashes);
        System.out.println(schemaStr);
        System.out.println(dashes);

        final List<Row> rows = getRows();
        if (numRows > maxNumToPrint) {
            final int numToPrint = maxNumToPrint / 2;
            for (Row r : rows.subList(0, numToPrint))  {
                r.prettyPrint(maxColNameLength);
            }
            System.out.println("...");
            for (Row r : rows.subList(numRows - numToPrint, numRows))  {
                r.prettyPrint(maxColNameLength);
            }
        } else {
            for (Row r : rows)  {
                r.prettyPrint(maxColNameLength);
            }
        }
        System.out.println(dashes);
        System.out.println();
    }

    /**
     * {@link #prettyPrint(int)} with default <tt>maxNumToPrint</tt> set to 10
     */
    public void prettyPrint() {
        prettyPrint(10);
    }

    // Fast Column-based methods
    public DataFrame addColumn(String colName, String[] colValues) {
        if (numRows == 0) {
            numRows = colValues.length;
        }

        schema.addColumn(Schema.ColType.STRING, colName);
        stringCols.add(colValues);
        indexToTypeIndex.add(stringCols.size()-1);
        return this;
    }

    public DataFrame addColumn(String colName, double[] colValues) {
        if (numRows == 0) {
            numRows = colValues.length;
        }

        schema.addColumn(ColType.DOUBLE, colName);
        doubleCols.add(colValues);
        indexToTypeIndex.add(doubleCols.size()-1);
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

    public boolean hasColumn(String columnName) { return schema.hasColumn(columnName); }

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
     * @return new DataFrame with copied rows
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
        for (DataFrame other : others) {
            n += other.numRows;
        }
        combined.numRows = n;
        int d = first.schema.getNumColumns();

        for (int colIdx = 0; colIdx < d; colIdx++) {
            Schema.ColType t = combined.schema.getColumnType(colIdx);
            if (t == Schema.ColType.STRING) {
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
            } else if (t == Schema.ColType.DOUBLE) {
                double[] newCol = new double[n];
                int i = 0;
                for (DataFrame curOther : others) {
                    double[] otherCol = curOther.getDoubleColumn(colIdx);
                    for (double curDouble : otherCol) {
                        newCol[i] = curDouble;
                        i++;
                    }
                }
                combined.doubleCols.add(newCol);
            } else {
                throw new MacrobaseInternalError("Invalid Col Type");
            }
        }

        return combined;
    }

    /**
     * @param projectionCols The columns that should be included in the returned DataFrame. Projections
     * that aren't in the columns of the current DataFrame will be ignored
     * @return return a new DataFrame that includes only the columns specified by @projections.
     */
    // TODO: write test for this method
    public DataFrame project(List<String> projectionCols) {
        final DataFrame other = new DataFrame();
        for (String col : projectionCols) {
            if (!schema.hasColumn(col)) {
                continue;
            }
            final ColType type = schema.getColumnTypeByName(col);
            if (type == ColType.DOUBLE) {
                other.addColumn(col, getDoubleColumnByName(col));
            } else if (type == ColType.STRING) {
                other.addColumn(col, getStringColumnByName(col));
            }
        }
        return other;
    }

    /**
     * @param mask rows to select
     * @return new DataFrame with subset of rows
     */
    public DataFrame filter(boolean[] mask) {
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
                other.addColumn(columnName, newColumn);
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
                other.addColumn(columnName, newColumn);
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
     * @return new DataFrame with subset of rows
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
     * @return new DataFrame with subset of rows
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
