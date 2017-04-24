package edu.stanford.futuredata.macrobase.datamodel;

import java.util.ArrayList;
import java.util.List;

/**
 * Format for import / export small batches
 */
public class Row {
    private Schema schema; // not set by user
    private List<Object> vals;

    public Row(Schema schema, List<Object> vals) {
        this.schema = schema;
        this.vals = vals;
    }
    public Row(List<Object> vals) {
        this.schema = null;
        this.vals = vals;
    }

    public List<Object> getVals() {
        return this.vals;
    }

    @SuppressWarnings("unchecked")
    public <T> T getAs(int i) {
        return (T)vals.get(i);
    }

    @SuppressWarnings("unchecked")
    public <T> T getAs(String colName) {
        if (schema == null) {
            throw new RuntimeException("No Schema");
        } else {
            return (T)vals.get(schema.getColumnIndex(colName));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Row row = (Row) o;
        return vals != null ? vals.equals(row.vals) : row.vals == null;
    }

    @Override
    public int hashCode() {
        return (vals != null ? vals.hashCode() : 0);
    }

    @Override
    public String toString() {
        ArrayList<String> strs = new ArrayList<>(vals.size());
        for (Object o : vals) {
            String s;
            if (o == null) {
                s = "NULL";
            } else {
                s = o.toString();
                if (o instanceof Double) {
                    if (s.length() > 7) {
                        double v = ((Double) o).doubleValue();
                        s = String.format("%.6g", v);
                    }
                }
            }
            strs.add(s);
        }
        return strs.toString();
    }
}
