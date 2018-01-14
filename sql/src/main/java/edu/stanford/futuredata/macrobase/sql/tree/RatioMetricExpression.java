package edu.stanford.futuredata.macrobase.sql.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class RatioMetricExpression extends Expression {

    private final Identifier funcName;
    private final AggregateExpression aggExpr;

    public RatioMetricExpression(Identifier funcName, AggregateExpression aggExpr) {
        this(Optional.empty(), funcName, aggExpr);
    }

    public RatioMetricExpression(NodeLocation location, Identifier funcName,
        AggregateExpression aggExpr) {
        this(Optional.of(location), funcName, aggExpr);
    }

    private RatioMetricExpression(Optional<NodeLocation> location, Identifier funcName,
        AggregateExpression aggExpr) {
        super(location);
        requireNonNull(funcName, "funcName is null");
        requireNonNull(aggExpr, "aggExpr is null");

        this.funcName = funcName;
        this.aggExpr = aggExpr;
    }

    public Identifier getFuncName() {
        return funcName;
    }

    public AggregateExpression getAggExpr() {
        return aggExpr;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRatioMetricExpression(this, context);
    }

    @Override
    public List<Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(aggExpr);
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
        RatioMetricExpression o = (RatioMetricExpression) obj;
        return Objects.equals(funcName, o.funcName) &&
            Objects.equals(aggExpr, o.aggExpr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(funcName, aggExpr);
    }
}
