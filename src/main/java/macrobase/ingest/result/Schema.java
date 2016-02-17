package macrobase.ingest.result;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Schema {
    public static class SchemaColumn {
        private String name;
        private String type;

        public SchemaColumn(String name, String type) {
            this.name = name;
            this.type = type;
        }

        @JsonProperty
        public String getName() {
            return name;
        }

        @JsonProperty
        public String getType() {
            return type;
        }

        public SchemaColumn() {
            // Jackson
        }
    }

    private List<SchemaColumn> columns;

    @JsonProperty
    public List<SchemaColumn> getColumns() {
        return columns;
    }

    public Schema(List<SchemaColumn> _columns) {
        columns = _columns;
    }

    public Schema() {
        // Jackson
    }
}
