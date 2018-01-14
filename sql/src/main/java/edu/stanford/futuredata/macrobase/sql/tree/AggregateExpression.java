package edu.stanford.futuredata.macrobase.sql.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class AggregateExpression extends Node {

    private final Aggregate agg;

    public AggregateExpression(Aggregate agg) {
        this(Optional.empty(), agg);
    }

    public AggregateExpression(NodeLocation location, Aggregate agg) {
        this(Optional.of(location), agg);
    }

    private AggregateExpression(Optional<NodeLocation> location, Aggregate agg) {
        super(location);
        requireNonNull(agg, "agg is null");

        this.agg = agg;
    }

    public Aggregate getAgg() {
        return agg;
    }

    @Override
    public List<Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(agg);
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
        AggregateExpression o = (AggregateExpression) obj;
        return Objects.equals(agg, o.agg);
    }

    @Override
    public String toString() {
        return agg.toString();
    }

    @Override
    public int hashCode() {
        return agg.hashCode();
    }
}
