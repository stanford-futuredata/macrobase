package edu.stanford.futuredata.macrobase.sql.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SplitQuery extends Node {

    private final Expression whereClause;
    private final Optional<Relation> relation;
    private final Optional<TableSubquery> subquery;

    public SplitQuery(Expression whereClause,
        Optional<Relation> relation, Optional<TableSubquery> subquery) {
        this(Optional.empty(), whereClause, relation, subquery);
    }

    public SplitQuery(NodeLocation location, Expression whereClause,
        Optional<Relation> relation,
        Optional<TableSubquery> subquery) {
        this(Optional.of(location), whereClause, relation, subquery);
    }

    private SplitQuery(Optional<NodeLocation> location, Expression whereClause,
        Optional<Relation> relation,
        Optional<TableSubquery> subquery) {
        super(location);
        requireNonNull(whereClause, "whereClause is null");
        requireNonNull(relation, "relation is null");
        requireNonNull(subquery, "subquery is null");

        this.whereClause = whereClause;
        this.relation = relation;
        this.subquery = subquery;
    }

    public Expression getWhereClause() {
        return whereClause;
    }

    public Relation getInputRelation() {
        return relation.orElseGet(subquery::get);
    }

    @Override
    public List<Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(whereClause);
        relation.ifPresent(nodes::add);
        subquery.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        SplitQuery o = (SplitQuery) obj;
        return Objects.equals(whereClause, o.whereClause) &&
            Objects.equals(relation, o.relation) &&
            Objects.equals(subquery, o.subquery);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("whereClause", whereClause)
            .add("relation", relation)
            .add("subquery", subquery)
            .omitNullValues()
            .toString();
    }

    @Override
    public int hashCode() {
        return Objects
            .hash(whereClause, relation, subquery);
    }
}
