package edu.stanford.futuredata.macrobase.sql.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SplitQuery extends Node {

    private final Identifier classifierName;
    private final List<Expression> classifierArgs;
    private final Optional<Relation> relation;
    private final Optional<TableSubquery> subquery;

    public SplitQuery(Identifier classifierName, List<Expression> classifierArgs,
        Optional<Relation> relation, Optional<TableSubquery> subquery) {
        this(Optional.empty(), classifierName, classifierArgs, relation, subquery);
    }

    public SplitQuery(NodeLocation location, Identifier classifierName,
        List<Expression> classifierArgs, Optional<Relation> relation,
        Optional<TableSubquery> subquery) {
        this(Optional.of(location), classifierName, classifierArgs, relation, subquery);
    }

    private SplitQuery(Optional<NodeLocation> location, Identifier classifierName,
        List<Expression> classifierArgs, Optional<Relation> relation,
        Optional<TableSubquery> subquery) {
        super(location);
        requireNonNull(classifierName, "classifierName is null");
        requireNonNull(classifierArgs, "classifierArgs is null");
        requireNonNull(relation, "relation is null");
        requireNonNull(subquery, "subquery is null");

        this.classifierName = classifierName;
        this.classifierArgs = classifierArgs;
        this.relation = relation;
        this.subquery = subquery;
    }

    public Identifier getClassifierName() {
        return classifierName;
    }

    public List<Expression> getClassifierArgs() {
        return classifierArgs;
    }

    public Relation getInputRelation() {
        return relation.orElseGet(subquery::get);
    }

    @Override
    public List<Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(classifierName);
        nodes.addAll(classifierArgs);
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
        return Objects.equals(classifierName, o.classifierName) &&
            Objects.equals(classifierArgs, o.classifierArgs) &&
            Objects.equals(relation, o.relation);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("classifierName", classifierName)
            .add("classierArgs", classifierArgs)
            .add("relation", relation)
            .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(classifierName, classifierArgs, relation);
    }
}
