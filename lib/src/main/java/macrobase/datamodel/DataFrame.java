package macrobase.datamodel;

import java.util.ArrayList;
import java.util.List;

import static macrobase.datamodel.Schema.ColType;

/**
 * Fast methods: addColumn
 */
public class DataFrame {
    private Schema schema;

    private ArrayList<String[]> stringCols;
    private ArrayList<double[]> doubleCols;
    private ArrayList<Integer> indexToTypeIndex = new ArrayList<>();

    private int numRows;

    public Schema getSchema() {return this.schema;}

    public DataFrame(Schema schema) {
        this.schema = schema;
        this.stringCols = new ArrayList<>();
        this.doubleCols = new ArrayList<>();
        this.indexToTypeIndex = new ArrayList<>();
        this.numRows = 0;
    }

    public DataFrame loadRows(List<Row> rows) {
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
                throw new RuntimeException("Invalid ColType");
            }
        }
        return this;
    }

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

    protected ArrayList<Integer> getColumnIndices(List<String> columns) {
        ArrayList<Integer> indices = new ArrayList<>(columns.size());
        for (String colName: columns) {
            indices.add(schema.getColumnIndex(colName));
        }
        return indices;
    }
    public ArrayList<double[]> getDoubleRowsByName(List<String> columns) {
        return getDoubleRows(getColumnIndices(columns));
    }
    public ArrayList<String[]> getStringRowsByName(List<String> columns) {
        return getStringRows(getColumnIndices(columns));
    }

    public Row getRow(int rowIdx) {
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
                throw new RuntimeException("Bad ColType");
            }
        }
        Row r = new Row(schema, rowValues);
        return r;
    }
}
