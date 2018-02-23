package edu.stanford.futuredata.macrobase.sql.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import edu.stanford.futuredata.macrobase.sql.parser.ParsingException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ExportClause extends Node {

    private final String fieldDelimiter;
    private final String lineDelimiter;
    private final String filename;

    public ExportClause(Optional<DelimiterClause> fieldDelimiter,
        Optional<DelimiterClause> lineDelimiter,
        String filename) {
        this(Optional.empty(), fieldDelimiter, lineDelimiter, filename);
    }

    public ExportClause(
        NodeLocation location,
        Optional<DelimiterClause> fieldDelimiter, Optional<DelimiterClause> lineDelimiter,
        String filename) {
        this(Optional.of(location), fieldDelimiter, lineDelimiter, filename);
    }

    private ExportClause(
        Optional<NodeLocation> location,
        Optional<DelimiterClause> fieldDelimiter, Optional<DelimiterClause> lineDelimiter,
        String filename) {
        super(location);
        requireNonNull(fieldDelimiter, "fieldDelimiter is null");
        requireNonNull(lineDelimiter, "lineDelimiter is null");
        requireNonNull(filename, "filename is null");

        this.fieldDelimiter = fieldDelimiter.orElse(new DelimiterClause(",")).toString();
        if (this.fieldDelimiter.length() != 1) {
            throw new ParsingException("FIELDS TERMINATED BY argument has length not equal to 1");
        }
        this.lineDelimiter = lineDelimiter.orElse(new DelimiterClause("\n")).toString();
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
        nodes.add(new DelimiterClause(fieldDelimiter))
            .add(new DelimiterClause(lineDelimiter))
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
        ExportClause o = (ExportClause) obj;
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
