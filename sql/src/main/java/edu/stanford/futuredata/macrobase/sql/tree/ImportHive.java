package edu.stanford.futuredata.macrobase.sql.tree;

import com.google.common.collect.ImmutableList;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.spark_project.guava.base.Objects.toStringHelper;

public class ImportHive extends Statement {

    private final String hiveQuery;
    private final QualifiedName tableName;
    private final Map<String, ColType> schema;

    public ImportHive(String hiveQuery, QualifiedName tableName, List<ColumnDefinition> columns) {
        this(Optional.empty(), hiveQuery, tableName, columns);
    }

    public ImportHive(NodeLocation location, String hiveQuery, QualifiedName tableName,
                     List<ColumnDefinition> columns) {
        this(Optional.of(location), hiveQuery, tableName, columns);
    }

    private ImportHive(Optional<NodeLocation> location, String hiveQuery, QualifiedName tableName,
                      List<ColumnDefinition> columns) {
        super(location);
        this.hiveQuery = requireNonNull(hiveQuery, "table is null");
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

    public String getHiveQuery() {
        return hiveQuery;
    }

    public QualifiedName getTableName() {
        return tableName;
    }

    public Map<String, ColType> getSchema() {
        return schema;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitHive(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public int hashCode() {
        return Objects.hash(hiveQuery, tableName, schema);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ImportHive o = (ImportHive) obj;
        return Objects.equals(hiveQuery, o.hiveQuery) &&
                Objects.equals(tableName, o.tableName) &&
                Objects.equals(schema, o.schema);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("hiveQuery", hiveQuery)
                .add("tableName", tableName)
                .add("columns", schema)
                .toString();
    }
}
