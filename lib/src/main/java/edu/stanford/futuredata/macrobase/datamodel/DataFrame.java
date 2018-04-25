package edu.stanford.futuredata.macrobase.datamodel;

import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsLast;
import static java.util.stream.Collectors.toList;

import com.google.common.base.Joiner;
import edu.stanford.futuredata.macrobase.analysis.summary.util.ModBitSet;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.util.MacroBaseInternalError;
import java.io.PrintStream;
import java.util.*;
import java.util.function.DoublePredicate;
import java.util.function.Predicate;
import java.util.stream.IntStream;
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

    private static final int MAX_COLS_FOR_TABULAR_PRINT = 10;

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
     * Slower than creating a DataFrame column by column using {@link #addColumn(String, double[])}
     * or {@link #addColumn(String, String[])}
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
                addStringColumnInternal(colValues);
            } else if (t == Schema.ColType.DOUBLE) {
                double[] colValues = new double[numRows];
                for (int i = 0; i < numRows; i++) {
                    colValues[i] = rows.get(i).<Double>getAs(c);
                }
                addDoubleColumnInternal(colValues);
            } else {
                throw new MacroBaseInternalError("Invalid ColType");
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
                throw new MacroBaseInternalError("Invalid ColType");
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        final DataFrame o = (DataFrame) obj;
        return Objects.equals(schema, o.schema) &&
            Objects.equals(numRows, o.numRows) &&
            Objects.equals(indexToTypeIndex, o.indexToTypeIndex) &&
            compareStringCols(stringCols, o.stringCols) &&
            compareDoubleCols(doubleCols, o.doubleCols);
    }

    /**
     * @return true if each String array in the first List contains the exact same values in the
     * same order as the second List
     */
    private boolean compareStringCols(final List<String[]> first, final List<String[]> second) {
        for (int i = 0; i < first.size(); ++i) {
            final String[] arr1 = first.get(i);
            final String[] arr2 = second.get(i);
            for (int j = 0; j < arr1.length; ++j) {
                if (!Objects.equals(arr1[j], arr2[j])) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * @return true if each double array in the first List contains the exact same values in the
     * same order as the second List
     */
    private boolean compareDoubleCols(final List<double[]> first, final List<double[]> second) {
        for (int i = 0; i < first.size(); ++i) {
            final double[] arr1 = first.get(i);
            final double[] arr2 = second.get(i);
            for (int j = 0; j < arr1.length; ++j) {
                if (arr1[j] != arr2[j]) {
                    return false;
                }
            }
        }
        return true;
    }

    public Schema getSchema() {return this.schema;}
    public int getNumRows() {return numRows;}
    public ArrayList<double[]> getDoubleCols() { return doubleCols; }
    public ArrayList<String[]> getStringCols() { return stringCols; }

    public String toString() {
        return getRows().toString();
    }

    /**
     * Pretty print contents of the DataFrame to STDOUT. Example outputs:
     * m rows
     *
     * ------------------------------------------
     * |   col_1  |   col_2  |  ...  |   col_n  |
     * ------------------------------------------
     * |  val_11  |  val_12  |  ...  |  val_1n  |
     * ...
     * |  val_m1  |  val_m2  |  ...  |  val_mn  |
     * ------------------------------------------
     *
     * or
     *
     * m rows
     *
     * col_1    |  val_11
     * col_2    |  val_12
     * ...
     * col_n    |  val_1n
     * -----------------------
     * ...
     * -----------------------
     * col_1    |  val_m1
     * col_2    |  val_m2
     * ...
     * col_n    |  val_mn
     * -----------------------
     *
     * @param out PrintStream to write to STDOUT or file (default: STDOUT)
     * @param maxNumToPrint maximum number of rows from the DataFrame to print (default: 20;
     * -1 prints out all rows)
     */
    public void prettyPrint(final PrintStream out, final int maxNumToPrint) {
        out.println(numRows +  (numRows == 1 ? " row" : " rows"));
        out.println();

        if (schema.getNumColumns() > MAX_COLS_FOR_TABULAR_PRINT) {
            // print each row so that each value is on a separate line, because the terminal isn't
            // wide enough to display the entire table
            if (maxNumToPrint > 0 && numRows > maxNumToPrint) {
                final int numToPrint = maxNumToPrint / 2;
                for (Row r : getRows(0, numToPrint))  {
                    r.prettyPrintColumnWise(out);
                }
                out.println();
                out.println("...");
                out.println();
                for (Row r : getRows(numRows - numToPrint, numRows))  {
                    r.prettyPrintColumnWise(out);
                }
            } else {
                for (Row r : getRows()) {
                    r.prettyPrintColumnWise(out);
                }
            }
        } else {
            // print DataFrame as a table
            final int maxColNameLength = schema.getColumnNames().stream()
                .reduce("", (x, y) -> x.length() > y.length() ? x : y).length();
            final int tableWidth =
                maxColNameLength + 4; // 2 extra spaces on both sides of each column name and value
            final List<String> colStrs = schema.getColumnNames().stream()
                .map((x) -> StringUtils.center(String.valueOf(x), tableWidth)).collect(toList());
            final String schemaStr = "|" + Joiner.on("|").join(colStrs) + "|";
            final String dashes = Joiner.on("").join(Collections.nCopies(schemaStr.length(), "-"));
            out.println(dashes);
            out.println(schemaStr);
            out.println(dashes);

            if (maxNumToPrint > 0 && numRows > maxNumToPrint) {
                final int numToPrint = maxNumToPrint / 2;
                for (Row r : getRows(0, numToPrint))  {
                    r.prettyPrint(out, tableWidth);
                }
                out.println();
                out.println("...");
                out.println();
                for (Row r : getRows(numRows - numToPrint, numRows))  {
                    r.prettyPrint(out, tableWidth);
                }
            } else {
                for (Row r : getRows())  {
                    r.prettyPrint(out, tableWidth);
                }
            }
            out.println(dashes);
            out.println();
        }
    }

    /**
     * {@link #prettyPrint(PrintStream, int)} with default <tt>out</tt> set to <tt>System.out</tt>
     * and <tt>maxNumToPrint</tt> set to 20
     */
    public void prettyPrint() {
        prettyPrint(System.out, 20);
    }

    /**
     * {@link #prettyPrint(PrintStream, int)} with default <tt>maxNumToPrint</tt> set to 20
     */
    public void prettyPrint(final PrintStream out) {
      prettyPrint(out, 20);
    }

    /**
     * {@link #prettyPrint(PrintStream, int)} with default <tt>out</tt> set to <tt>System.out</tt>
     */
    public void prettyPrint(final int maxNumToPrint) {
        prettyPrint(System.out, maxNumToPrint);
    }

    // Fast Column-based methods
    public DataFrame addColumn(String colName, String[] colValues) {
        if (numRows == 0) {
            numRows = colValues.length;
        }

        schema.addColumn(Schema.ColType.STRING, colName);
        addStringColumnInternal(colValues);
        return this;
    }

    public DataFrame addColumn(String colName, double[] colValues) {
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

    private void addDoubleColumnInternal(double[] colValues) {
        doubleCols.add(colValues);
        indexToTypeIndex.add(doubleCols.size()-1);
    }

    protected int[] getSubIndices(List<Integer> columns) {
        int d = columns.size();
        int[] typeSubIndices = new int[d];
        for (int i = 0; i < d; i++) {
            typeSubIndices[i] = indexToTypeIndex.get(columns.get(i));
        }
        return typeSubIndices;
    }

    /**
     * Rename column in schema.
     *
     * @param oldColumnName The name of the column to be renamed. If it doesn't exist, nothing is
     * changed
     * @param newColumnName The new name for the column
     * @return true if rename was successful, false otherwise
     */
    public boolean renameColumn(final String oldColumnName, final String newColumnName) {
        return schema.renameColumn(oldColumnName, newColumnName);
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
                throw new MacroBaseInternalError("Invalid Col Type");
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
    public DataFrame filter(ModBitSet mask) {
        DataFrame other = new DataFrame();

        int d = schema.getNumColumns();
        int numTrue = 0;
        for (int i = 0; i < numRows; i++) {
            if (mask.get(i)) {
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
                    if (mask.get(i)) {
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
                    if (mask.get(i)) {
                        newColumn[j] = oldColumn[i];
                        j++;
                    }
                }
                other.addColumn(columnName, newColumn);
            } else {
                throw new MacroBaseInternalError("Bad Column Type");
            }
        }
        return other;
    }

    public DataFrame filter(int columnIdx, Predicate<Object> filter) {
        String[] filterColumn = getStringColumn(columnIdx);
        final ModBitSet mask = new ModBitSet(numRows);
        for (int i = 0; i < numRows; i++) {
          mask.set(i, filter.test(filterColumn[i]));
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
        final ModBitSet mask = getMaskForFilter(columnIdx, filter);
        return filter(mask);
    }

    /**
     * @param columnIdx column index to filter by
     * @param filter Predicate<Object> to test each column value
     * @return a ModBitSet that encodes the true/false value generated by the filter
     * on each row in the DataFrame
     */
    public ModBitSet getMaskForFilter(int columnIdx, Predicate<Object> filter) {
        String[] filterColumn = getStringColumn(columnIdx);
        final ModBitSet mask = new ModBitSet(numRows);
        for (int i = 0; i < numRows; i++) {
          mask.set(i, filter.test(filterColumn[i]));
        }
        return mask;
    }

    /**
     * @param columnIdx column index to filter by
     * @param filter DoublePredicate to test each column value
     * @return a ModBitSet that encodes the true/false value generated by the filter
     * on each row in the DataFrame
     */
    public ModBitSet getMaskForFilter(int columnIdx, DoublePredicate filter) {
        double[] filterColumn = getDoubleColumn(columnIdx);
        final ModBitSet mask = new ModBitSet(numRows);
        for (int i = 0; i < numRows; i++) {
            mask.set(i, filter.test(filterColumn[i]));
        }
        return mask;
    }

    /**
     * @param columnName column name to filter by
     * @param filter predicate to test each column value
     * @return new DataFrame with subset of rows
     */
    public DataFrame filter(String columnName, DoublePredicate filter) {
        return filter(schema.getColumnIndex(columnName), filter);
    }

    /**
     * {@link #limit(int)} with default <tt>numRows</tt> set to -1 (i.e., LIMIT ALL)
     * @return this DataFrame, unchanged
     */
    public DataFrame limit() {
        return limit(-1);
    }

    /**
     * Execute the LIMIT clause of a SQL query, i.e., take the first n rows of the DataFrame
     * @param numRows Number of rows to include the new DataFrame. If -1, return the original
     * DataFrame
     * @return the new DataFrame with only the first <tt>numRows</tt> rows.
     */
    public DataFrame limit(final int numRows) {
      if (numRows < 0 || numRows >= this.numRows) {
          return this;
      }
      final DataFrame result = new DataFrame();
      result.schema = this.schema.copy();
      result.indexToTypeIndex = new ArrayList<>(this.indexToTypeIndex);
      result.numRows = numRows;
      final int numColumns = this.schema.getNumColumns();

      for (int colIdx = 0; colIdx < numColumns; colIdx++) {
          Schema.ColType t = result.schema.getColumnType(colIdx);
          if (t == Schema.ColType.STRING) {
              final String[] col = this.getStringColumn(colIdx);
              final String[] newCol = new String[numRows];
              for (int i = 0; i < numRows; ++i) {
                  newCol[i] = col[i];
              }
              result.stringCols.add(newCol);
          } else if (t == Schema.ColType.DOUBLE) {
              final double[] col = this.getDoubleColumn(colIdx);
              final double[] newCol = new double[numRows];
              for (int i = 0; i < numRows; ++i) {
                  newCol[i] = col[i];
              }
              result.doubleCols.add(newCol);
          }
      }
      return result;
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
                throw new MacroBaseInternalError("Bad ColType");
            }
        }
        Row r = new Row(schema, rowValues);
        return r;
    }

    public List<Row> getRows() {
        return getRows(0, numRows);
    }

    private List<Row> getRows(final int startIndex, int numRowsToGet) {
        if (numRowsToGet > numRows) {
            numRowsToGet = numRows;
        }
        List<Row> rows = new ArrayList<>();
        for (int rowIdx = startIndex; rowIdx < numRowsToGet; rowIdx++) {
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

    /**
     * Sort DataFrame rows by a single column.
     * // TODO: write test for OrderBy
     * @param sortCol The column to sort by
     * @param sortAsc True => sort ascending, False => sort descending
     * @return A new DataFrame with the correct sorted order. If <tt>col</tt> is
     * not in the DataFrame's schema, return the same DataFrame, unchanged
     */
    public DataFrame orderBy(final String sortCol, final boolean sortAsc) {
      // If column is not present in DataFrame, return as is
      if (!this.schema.hasColumn(sortCol)) {
          return this;
      }

      final DataFrame sortedDf = new DataFrame();
      final ColType sortColType = this.schema.getColumnTypeByName(sortCol);
      final int numColumns = this.schema.getNumColumns();

      if (sortColType == ColType.DOUBLE) {
          sortColumns(sortedDf, numColumns, getDoubleColumnByName(sortCol), sortAsc);
      } else {
          sortColumns(sortedDf, numColumns, getStringColumnByName(sortCol), sortAsc);
      }
      return sortedDf;
    }

    // TODO: this code duplication is awful, gotta think of a better way of doing this
    private void sortColumns(final DataFrame sortedDf, final int numColumns,
        final double[] sortColumn, final boolean sortAsc) {
        Comparator<Integer> comparator = comparing(i -> sortColumn[i], nullsLast(
            naturalOrder()));
        if (!sortAsc) {
            comparator = comparator.reversed();
        }
        for (int c = 0; c < numColumns; ++c) {
            if (this.schema.getColumnType(c) == ColType.DOUBLE) {
                final double[] origCol = this.getDoubleColumn(c);
                final double[] newCol = IntStream.range(0, origCol.length).boxed()
                    .sorted(comparator)
                    .mapToDouble(i -> origCol[i]).toArray();
                sortedDf.addColumn(this.schema.getColumnName(c), newCol);
            } else {
                // ColType.STRING
                final String[] origCol = this.getStringColumn(c);
                final String[] newCol = IntStream.range(0, origCol.length).boxed()
                    .sorted(comparator).map(i -> origCol[i])
                    .toArray(String[]::new);
                sortedDf.addColumn(this.schema.getColumnName(c), newCol);
            }
        }
    }

    private void sortColumns(final DataFrame sortedDf, final int numColumns,
        final String[] sortColumn, final boolean sortAsc) {
        Comparator<Integer> comparator = comparing(i -> sortColumn[i], nullsLast(naturalOrder()));
        if (!sortAsc) {
            comparator = comparator.reversed();
        }
        for (int c = 0; c < numColumns; ++c) {
            if (this.schema.getColumnType(c) == ColType.DOUBLE) {
                final double[] origCol = this.getDoubleColumn(c);
                final double[] newCol = IntStream.range(0, origCol.length).boxed()
                    .sorted(comparator)
                    .mapToDouble(i -> origCol[i]).toArray();
                sortedDf.addColumn(this.schema.getColumnName(c), newCol);
            } else {
                // ColType.STRING
                final String[] origCol = this.getStringColumn(c);
                final String[] newCol = IntStream.range(0, origCol.length).boxed()
                    .sorted(comparator).map(i -> origCol[i])
                    .toArray(String[]::new);
                sortedDf.addColumn(this.schema.getColumnName(c), newCol);
            }
        }
    }

}
