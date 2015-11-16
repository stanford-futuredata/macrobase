package macrobase.ingest.result;

public class ColumnValue {
    private String column;
    private String value;

    public ColumnValue(String column, String value) {
        this.column = column;
        this.value = value;
    }

    public String getColumn() {
        return column;
    }

    public String getValue() {
        return value;
    }

    public ColumnValue() {
        // JACKSON
    }
}
