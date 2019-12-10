package edu.stanford.futuredata.macrobase.sql.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DiffQuerySpecification extends QueryBody {

    private final Select select;
    private final boolean anti;
    // Either first and second are present, or splitQuery is
    private final Optional<TableSubquery> first;
    private final Optional<TableSubquery> second;
    private final Optional<SplitQuery> splitQuery;
    // Required
    private final List<Identifier> attributeCols;
    // Optional, with defaults
    private final MinRatioExpression minRatioExpr;
    private final MinSupportExpression minSupportExpr;
    private final RatioMetricExpression ratioMetricExpr;
    private final IntLiteral maxCombo;
    // Optional
    private final Optional<Expression> where;
    private final Optional<OrderBy> orderBy;
    private final Optional<String> limit;
    private final Optional<ExportClause> exportExpr;

    private static final IntLiteral DEFAULT_MAX_COMBO = new IntLiteral("3");
    private static final MinRatioExpression DEFAULT_MIN_RATIO_EXPRESSION = new MinRatioExpression(
        new DecimalLiteral("1.5"));
    private static final MinSupportExpression DEFAULT_MIN_SUPPORT_EXPRESSION = new MinSupportExpression(
        new DecimalLiteral("0.2"));
    private static final RatioMetricExpression DEFAULT_RATIO_METRIC_EXPRESSION =
        new RatioMetricExpression(new Identifier("global_ratio"),
            new AggregateExpression(new Aggregate("COUNT")));

    public DiffQuerySpecification(
        Select select,
        boolean anti,
        Optional<TableSubquery> first,
        Optional<TableSubquery> second,
        Optional<SplitQuery> splitQuery,
        List<Identifier> attributeCols,
        Optional<MinRatioExpression> minRatioExpr,
        Optional<MinSupportExpression> minSupportExpr,
        Optional<RatioMetricExpression> ratioMetricExpr,
        Optional<IntLiteral> maxCombo,
        Optional<Expression> where,
        Optional<OrderBy> orderBy,
        Optional<String> limit,
        Optional<ExportClause> exportExpr) {
        this(Optional.empty(), select, anti, first, second, splitQuery, attributeCols, minRatioExpr,
            minSupportExpr, ratioMetricExpr, maxCombo, where, orderBy, limit, exportExpr);
    }

    public DiffQuerySpecification(
        NodeLocation location,
        Select select,
        boolean anti,
        Optional<TableSubquery> first,
        Optional<TableSubquery> second,
        Optional<SplitQuery> splitQuery,
        List<Identifier> attributeCols,
        Optional<MinRatioExpression> minRatioExpr,
        Optional<MinSupportExpression> minSupportExpr,
        Optional<RatioMetricExpression> ratioMetricExpr,
        Optional<IntLiteral> maxCombo,
        Optional<Expression> where,
        Optional<OrderBy> orderBy,
        Optional<String> limit,
        Optional<ExportClause> exportExpr) {
        this(Optional.of(location), select, anti, first, second, splitQuery, attributeCols, minRatioExpr,
            minSupportExpr, ratioMetricExpr, maxCombo, where, orderBy, limit, exportExpr);
    }

    private DiffQuerySpecification(
        Optional<NodeLocation> location,
        Select select,
        boolean anti,
        Optional<TableSubquery> first,
        Optional<TableSubquery> second,
        Optional<SplitQuery> splitQuery,
        List<Identifier> attributeCols,
        Optional<MinRatioExpression> minRatioExpr,
        Optional<MinSupportExpression> minSupportExpr,
        Optional<RatioMetricExpression> ratioMetricExpr,
        Optional<IntLiteral> maxCombo,
        Optional<Expression> where,
        Optional<OrderBy> orderBy,
        Optional<String> limit,
        Optional<ExportClause> exportExpr) {
        super(location);
        requireNonNull(select, "select is null");
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");
        requireNonNull(splitQuery, "splitQuery is null");
        requireNonNull(attributeCols, "attributeCols is null");
        requireNonNull(minRatioExpr, "minRatioExpr is null");
        requireNonNull(minSupportExpr, "minSupportExpr is null");
        requireNonNull(ratioMetricExpr, "ratioMetricExpr is null");
        requireNonNull(maxCombo, "maxCombo is null");
        requireNonNull(where, "where is null");
        requireNonNull(orderBy, "orderBy is null");
        requireNonNull(limit, "limit is null");
        requireNonNull(exportExpr, "exportExpr is null");

        this.select = select;
        this.anti = anti;
        this.first = first;
        this.second = second;
        this.splitQuery = splitQuery;
        this.attributeCols = attributeCols;
        this.minRatioExpr = minRatioExpr.orElse(DEFAULT_MIN_RATIO_EXPRESSION);
        this.minSupportExpr = minSupportExpr.orElse(DEFAULT_MIN_SUPPORT_EXPRESSION);
        this.ratioMetricExpr = ratioMetricExpr.orElse(DEFAULT_RATIO_METRIC_EXPRESSION);
        this.maxCombo = maxCombo.orElse(DEFAULT_MAX_COMBO);
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
        this.exportExpr = exportExpr;
    }

    public Select getSelect() {
        return select;
    }

    public boolean isAnti() { return anti; }

    public Optional<TableSubquery> getFirst() {
        return first;
    }

    public Optional<TableSubquery> getSecond() {
        return second;
    }

    public boolean hasTwoArgs() {
        return first.isPresent() && second.isPresent();
    }

    public Optional<SplitQuery> getSplitQuery() {
        return splitQuery;
    }

    public List<Identifier> getAttributeCols() {
        return attributeCols;
    }

    public MinRatioExpression getMinRatioExpression() {
        return minRatioExpr;
    }

    public MinSupportExpression getMinSupportExpression() {
        return minSupportExpr;
    }

    public RatioMetricExpression getRatioMetricExpr() {
        return ratioMetricExpr;
    }

    public IntLiteral getMaxCombo() {
        return maxCombo;
    }

    public Optional<Expression> getWhere() {
        return where;
    }

    public Optional<OrderBy> getOrderBy() {
        return orderBy;
    }

    public Optional<String> getLimit() {
        return limit;
    }

    public Optional<ExportClause> getExportExpr() {
        return exportExpr;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDiffQuerySpecification(this, context);
    }

    @Override
    public List<Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(select);
        if (anti) {
            nodes.add(new StringLiteral("ANTI"));
        }
        first.ifPresent(nodes::add);
        second.ifPresent(nodes::add);
        nodes.addAll(attributeCols);
        nodes.add(minRatioExpr);
        nodes.add(minSupportExpr);
        nodes.add(ratioMetricExpr);
        nodes.add(new IntLiteral("" + maxCombo));
        where.ifPresent(nodes::add);
        orderBy.ifPresent(nodes::add);
        limit.ifPresent((str) -> nodes.add(new StringLiteral(str)));
        exportExpr.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("select", select)
            .add("anti", anti)
            .add("first", first)
            .add("second", second.orElse(null))
            .add("attributeCols", attributeCols)
            .add("minRatioExpr", minRatioExpr)
            .add("minSupportExpr", minSupportExpr)
            .add("ratioMetricExpr", ratioMetricExpr)
            .add("maxCombo", maxCombo)
            .add("where", where.orElse(null))
            .add("orderBy", orderBy)
            .add("limit", limit.orElse(null))
            .add("exportExpr", exportExpr.orElse(null))
            .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        DiffQuerySpecification o = (DiffQuerySpecification) obj;
        return Objects.equals(select, o.select) &&
            Objects.equals(anti, o.anti) &&
            Objects.equals(first, o.first) &&
            Objects.equals(second, o.second) &&
            Objects.equals(attributeCols, o.attributeCols) &&
            Objects.equals(minRatioExpr, o.minRatioExpr) &&
            Objects.equals(minSupportExpr, o.minSupportExpr) &&
            Objects.equals(ratioMetricExpr, o.ratioMetricExpr) &&
            Objects.equals(maxCombo, o.maxCombo) &&
            Objects.equals(where, o.where) &&
            Objects.equals(orderBy, o.orderBy) &&
            Objects.equals(limit, o.limit) &&
            Objects.equals(exportExpr, o.exportExpr);
    }

    @Override
    public int hashCode() {
        return Objects
            .hash(select, anti, first, second, attributeCols, minRatioExpr, minSupportExpr,
                ratioMetricExpr,
                maxCombo, where, orderBy,
                limit, exportExpr);
    }
}
