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
  private final Relation relation;

  public SplitQuery(Identifier classifierName, List<Expression> classifierArgs, Relation relation) {
    this(Optional.empty(), classifierName, classifierArgs, relation);
  }

  public SplitQuery(NodeLocation location, Identifier classifierName,
      List<Expression> classifierArgs, Relation relation) {
    this(Optional.of(location), classifierName, classifierArgs, relation);
  }

  private SplitQuery(Optional<NodeLocation> location, Identifier classifierName,
      List<Expression> classifierArgs, Relation relation) {
    super(location);
    requireNonNull(classifierName, "classifierName is null");
    requireNonNull(classifierArgs, "classifierArgs is null");
    requireNonNull(relation, "relation is null");

    this.classifierName = classifierName;
    this.classifierArgs = classifierArgs;
    this.relation = relation;
  }

  public Identifier getClassifierName() {
    return classifierName;
  }

  public List<Expression> getClassifierArgs() {
    return classifierArgs;
  }

  public Relation getRelation() {
    return relation;
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.add(classifierName);
    nodes.addAll(classifierArgs);
    nodes.add(relation);
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
