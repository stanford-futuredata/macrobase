package macrobase.ingest.result;

import org.json.JSONException;
import org.json.JSONObject;

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

    @Override
    public int hashCode() {
        return column.hashCode()+value.hashCode();
    }

    public JSONObject toJsonObject() {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(column, value);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject;
    }
}
