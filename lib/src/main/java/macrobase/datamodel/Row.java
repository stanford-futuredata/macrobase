package macrobase.datamodel;

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
        return (T)vals.get(schema.getColumnIndex(colName));
    }
}
