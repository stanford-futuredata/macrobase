package edu.stanford.futuredata.macrobase.sql.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;

public class MinRatioExpression extends Node {

    private final DecimalLiteral minRatio;

    public MinRatioExpression(DecimalLiteral minRatio) {
        this(Optional.empty(), minRatio);
    }

    public MinRatioExpression(NodeLocation location, DecimalLiteral minRatio) {
        this(Optional.of(location), minRatio);
    }

    private MinRatioExpression(Optional<NodeLocation> location, DecimalLiteral minRatio) {
        super(location);
        requireNonNull(minRatio, "minRatio is null");
        this.minRatio = minRatio;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(minRatio);
        return nodes.build();
    }

    @Override
    public int hashCode() {
        return minRatio.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MinRatioExpression o = (MinRatioExpression) obj;
        return o.minRatio.equals(minRatio);
    }

    @Override
    public String toString() {
        return minRatio.toString();
    }

    public double getMinRatio() {
        return minRatio.getValue();
    }
}
