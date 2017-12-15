package edu.stanford.futuredata.macrobase.sql.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ExportExpression extends Node {

  private final String filename;

  public ExportExpression(String filename) {
    this(Optional.empty(), filename);
  }

  public ExportExpression(
      NodeLocation location,
      String filename) {
    this(Optional.of(location), filename);
  }

  private ExportExpression(
      Optional<NodeLocation> location,
      String filename) {
    super(location);
    requireNonNull(filename, "filename is null");

    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  @Override
  public List<? extends Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.add(new StringLiteral(filename));
    return nodes.build();
  }

  @Override
  public int hashCode() {
    return Objects.hash(filename);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    ExportExpression o = (ExportExpression) obj;
    return Objects.equals(filename, o.filename);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("filename", filename)
        .toString();
  }
}
