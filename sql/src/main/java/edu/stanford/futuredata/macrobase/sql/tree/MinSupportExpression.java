package edu.stanford.futuredata.macrobase.sql.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;

public class MinSupportExpression extends Node {

    private final DecimalLiteral minSupport;

    public MinSupportExpression(DecimalLiteral minSupport) {
        this(Optional.empty(), minSupport);
    }

    public MinSupportExpression(NodeLocation location, DecimalLiteral minSupport) {
        this(Optional.of(location), minSupport);
    }

    private MinSupportExpression(Optional<NodeLocation> location, DecimalLiteral minSupport) {
        super(location);
        requireNonNull(minSupport, "minSupport is null");
        this.minSupport = minSupport;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(minSupport);
        return nodes.build();
    }

    @Override
    public int hashCode() {
        return minSupport.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MinSupportExpression o = (MinSupportExpression) obj;
        return o.minSupport.equals(minSupport);
    }

    @Override
    public String toString() {
        return minSupport.toString();
    }

    public double getMinSupport() {
        return minSupport.getValue();
    }
}
