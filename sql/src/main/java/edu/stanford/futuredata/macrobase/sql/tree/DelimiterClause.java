package edu.stanford.futuredata.macrobase.sql.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringEscapeUtils;

public class DelimiterClause extends Node {

    private final String delimiter;

    public DelimiterClause(String delimiter) {
        this(Optional.empty(), delimiter);
    }

    public DelimiterClause(NodeLocation location, String delimiter) {
        this(Optional.of(location), delimiter);
    }

    private DelimiterClause(Optional<NodeLocation> location, String delimiter) {
        super(location);
        requireNonNull(delimiter, "delimiter is null");
        this.delimiter = StringEscapeUtils.unescapeJava(delimiter);
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

        DelimiterClause o = (DelimiterClause) obj;
        return o.delimiter.equals(delimiter);
    }

    @Override
    public String toString() {
        return delimiter;
    }
}
