package edu.stanford.futuredata.macrobase.sql.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ExportExpression extends Node {

  private final String fieldDelimiter;
  private final String lineDelimiter;
  private final String filename;

  public ExportExpression(Optional<DelimiterExpression> fieldDelimiter,
      Optional<DelimiterExpression> lineDelimiter,
      String filename) {
    this(Optional.empty(), fieldDelimiter, lineDelimiter, filename);
  }

  public ExportExpression(
      NodeLocation location,
      Optional<DelimiterExpression> fieldDelimiter, Optional<DelimiterExpression> lineDelimiter,
      String filename) {
    this(Optional.of(location), fieldDelimiter, lineDelimiter, filename);
  }

  private ExportExpression(
      Optional<NodeLocation> location,
      Optional<DelimiterExpression> fieldDelimiter, Optional<DelimiterExpression> lineDelimiter,
      String filename) {
    super(location);
    requireNonNull(fieldDelimiter, "fieldDelimiter is null");
    requireNonNull(lineDelimiter, "lineDelimiter is null");
    requireNonNull(filename, "filename is null");

    this.fieldDelimiter = fieldDelimiter.orElse(new DelimiterExpression(",")).toString();
    if (this.fieldDelimiter.length() != 1) {
      throw new IllegalArgumentException("fieldDelimiter's length not equal to 1");
    }
    this.lineDelimiter = lineDelimiter.orElse(new DelimiterExpression("\n")).toString();
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  public String getFieldDelimiter() {
    return fieldDelimiter;
  }

  public String getLineDelimiter() {
    return lineDelimiter;
  }

  @Override
  public List<? extends Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.add(new DelimiterExpression(fieldDelimiter))
        .add(new DelimiterExpression(lineDelimiter))
        .add(new StringLiteral(filename));
    return nodes.build();
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldDelimiter, lineDelimiter, filename);
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
    return Objects.equals(fieldDelimiter, o.fieldDelimiter) &&
        Objects.equals(lineDelimiter, o.lineDelimiter) &&
        Objects.equals(filename, o.filename);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("fieldDelimiter", fieldDelimiter)
        .add("lineDelimiter", lineDelimiter)
        .add("filename", filename)
        .toString();
  }
}
