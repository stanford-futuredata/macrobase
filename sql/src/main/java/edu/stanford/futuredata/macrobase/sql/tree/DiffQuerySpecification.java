package edu.stanford.futuredata.macrobase.sql.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DiffQuerySpecification extends QueryBody {

  private final Select select;
  private final Optional<Query> first;
  private final Optional<Query> second;
  private final List<Identifier> attributeCols;
  private final Optional<MinRatioExpression> minRatioExpr;
  private final Optional<MinSupportExpression> minSupportExpr;
  private final Optional<RatioMetricExpression> ratioMetricExpr;
  private final Optional<LongLiteral> maxCombo;
  private final Optional<Expression> where;
  private final Optional<OrderBy> orderBy;
  private final Optional<String> limit;
  private final Optional<ExportExpression> exportExpr;

  private static final LongLiteral DEFAULT_MAX_COMBO = new LongLiteral("3");
  private static final MinRatioExpression DEFAULT_MIN_RATIO_EXPRESSION = new MinRatioExpression(
      new DecimalLiteral("1.5"));
  private static final MinSupportExpression DEFAULT_MIN_SUPPORT_EXPRESSION = new MinSupportExpression(
      new DecimalLiteral("0.2"));
  private static final RatioMetricExpression DEFAULT_RATIO_METRIC_EXPRESSION =
      new RatioMetricExpression(new Identifier("global_ratio"),
          new AggregateExpression(new Aggregate("COUNT")));

  public DiffQuerySpecification(
      Select select,
      Optional<Query> first,
      Optional<Query> second,
      List<Identifier> attributeCols,
      Optional<MinRatioExpression> minRatioExpr,
      Optional<MinSupportExpression> minSupportExpr,
      Optional<RatioMetricExpression> ratioMetricExpr,
      Optional<LongLiteral> maxCombo,
      Optional<Expression> where,
      Optional<OrderBy> orderBy,
      Optional<String> limit,
      Optional<ExportExpression> exportExpr) {
    this(Optional.empty(), select, first, second, attributeCols, minRatioExpr, minSupportExpr,
        ratioMetricExpr, maxCombo, where,
        orderBy, limit, exportExpr);
  }

  public DiffQuerySpecification(
      NodeLocation location,
      Select select,
      Optional<Query> first,
      Optional<Query> second,
      List<Identifier> attributeCols,
      Optional<MinRatioExpression> minRatioExpr,
      Optional<MinSupportExpression> minSupportExpr,
      Optional<RatioMetricExpression> ratioMetricExpr,
      Optional<LongLiteral> maxCombo,
      Optional<Expression> where,
      Optional<OrderBy> orderBy,
      Optional<String> limit,
      Optional<ExportExpression> exportExpr) {
    this(Optional.of(location), select, first, second, attributeCols, minRatioExpr, minSupportExpr,
        ratioMetricExpr, maxCombo,
        where, orderBy, limit, exportExpr);
  }

  private DiffQuerySpecification(
      Optional<NodeLocation> location,
      Select select,
      Optional<Query> first,
      Optional<Query> second,
      List<Identifier> attributeCols,
      Optional<MinRatioExpression> minRatioExpr,
      Optional<MinSupportExpression> minSupportExpr,
      Optional<RatioMetricExpression> ratioMetricExpr,
      Optional<LongLiteral> maxCombo,
      Optional<Expression> where,
      Optional<OrderBy> orderBy,
      Optional<String> limit,
      Optional<ExportExpression> exportExpr) {
    super(location);
    requireNonNull(select, "select is null");
    requireNonNull(first, "first is null");
    requireNonNull(second, "second is null");
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
    this.first = first;
    this.second = second;
    this.attributeCols = attributeCols;
    this.minRatioExpr = Optional.of(minRatioExpr.orElse(DEFAULT_MIN_RATIO_EXPRESSION));
    this.minSupportExpr = Optional.of(minSupportExpr.orElse(DEFAULT_MIN_SUPPORT_EXPRESSION));
    this.ratioMetricExpr = Optional.of(ratioMetricExpr.orElse(DEFAULT_RATIO_METRIC_EXPRESSION));
    this.maxCombo = Optional.of(maxCombo.orElse(DEFAULT_MAX_COMBO));
    this.where = where;
    this.orderBy = orderBy;
    this.limit = limit;
    this.exportExpr = exportExpr;
  }

  public Select getSelect() {
    return select;
  }

  public Optional<Query> getFirst() {
    return first;
  }

  public Optional<Query> getSecond() {
    return second;
  }

  public List<Identifier> getAttributeCols() {
    return attributeCols;
  }

  public Optional<MinRatioExpression> getMinRatioExpression() {
    return minRatioExpr;
  }

  public Optional<MinSupportExpression> getMinSupportExpression() {
    return minSupportExpr;
  }

  public Optional<RatioMetricExpression> getRatioMetricExpr() {
    return ratioMetricExpr;
  }

  public Optional<LongLiteral> getMaxCombo() {
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

  public Optional<ExportExpression> getExportExpr() {
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
    first.ifPresent(nodes::add);
    second.ifPresent(nodes::add);
    nodes.addAll(attributeCols);
    nodes.add(minRatioExpr.get());
    nodes.add(minSupportExpr.get());
    nodes.add(ratioMetricExpr.get());
    nodes.add(new LongLiteral("" + maxCombo));
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
        .hash(select, first, second, attributeCols, minRatioExpr, minSupportExpr, ratioMetricExpr,
            maxCombo, where, orderBy,
            limit, exportExpr);
  }
}
