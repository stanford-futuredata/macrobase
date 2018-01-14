package edu.stanford.futuredata.macrobase.sql.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;

public class Aggregate extends Node {

    public enum AggEnum {
        COUNT,
        MIN,
        MAX,
        SUM,
    }

    private final AggEnum value;

    public Aggregate(String value) {
        this(Optional.empty(), value);
    }

    public Aggregate(NodeLocation location, String value) {
        this(Optional.of(location), value);
    }

    private Aggregate(Optional<NodeLocation> location, String value) {
        super(location);
        requireNonNull(value, "value is null");
        this.value = AggEnum.valueOf(value);
    }

    public AggEnum getValue() {
        return value;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(new StringLiteral(value.toString()));
        return nodes.build();
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Aggregate o = (Aggregate) obj;
        return o.value == value;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
