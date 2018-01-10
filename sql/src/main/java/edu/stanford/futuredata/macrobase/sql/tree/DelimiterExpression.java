package edu.stanford.futuredata.macrobase.sql.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringEscapeUtils;

public class DelimiterExpression extends Node {

  private final String delimiter;

  public DelimiterExpression(String delimiter) {
    this(Optional.empty(), delimiter);
  }

  public DelimiterExpression(NodeLocation location, String delimiter) {
    this(Optional.of(location), delimiter);
  }

  private DelimiterExpression(Optional<NodeLocation> location, String delimiter) {
    super(location);
    requireNonNull(delimiter, "delimiter is null");
    this.delimiter = StringEscapeUtils.unescapeJava(delimiter);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDelimiterExpression(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.add(new StringLiteral(delimiter.toString()));
    return nodes.build();
  }

  @Override
  public int hashCode() {
    return delimiter.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    DelimiterExpression o = (DelimiterExpression) obj;
    return o.delimiter.equals(delimiter);
  }

  @Override
  public String toString() {
    return delimiter;
  }
}
