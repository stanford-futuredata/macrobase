package edu.stanford.futuredata.macrobase.sql.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class ImportCsv extends Statement {

    private final String filename;
    private final QualifiedName tableName;
    private final Map<String, ColType> schema;

    public ImportCsv(String filename, QualifiedName tableName, List<ColumnDefinition> columns) {
        this(Optional.empty(), filename, tableName, columns);
    }

    public ImportCsv(NodeLocation location, String filename, QualifiedName tableName,
        List<ColumnDefinition> columns) {
        this(Optional.of(location), filename, tableName, columns);
    }

    private ImportCsv(Optional<NodeLocation> location, String filename, QualifiedName tableName,
        List<ColumnDefinition> columns) {
        super(location);
        this.filename = requireNonNull(filename, "table is null");
        this.tableName = requireNonNull(tableName, "where is null");
        requireNonNull(columns, "columns is null");
        this.schema = columns.stream()
            .collect(Collectors.toMap(x -> x.getName().getValue(), this::getColType));
    }

    private ColType getColType(final ColumnDefinition colDef) {
        switch (colDef.getType()) {
            case "string":
                return ColType.STRING;
            case "double":
                return ColType.DOUBLE;
        }
        return ColType.STRING; //
    }

    public String getFilename() {
        return filename;
    }

    public QualifiedName getTableName() {
        return tableName;
    }

    public Map<String, ColType> getSchema() {
        return schema;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitImportCsv(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, tableName, schema);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ImportCsv o = (ImportCsv) obj;
        return Objects.equals(filename, o.filename) &&
            Objects.equals(tableName, o.tableName) &&
            Objects.equals(schema, o.schema);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("filename", filename)
            .add("tableName", tableName)
            .add("columns", schema)
            .toString();
    }
}

