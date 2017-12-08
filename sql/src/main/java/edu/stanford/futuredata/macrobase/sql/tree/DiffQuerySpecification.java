package edu.stanford.futuredata.macrobase.sql.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DiffQuerySpecification
    extends QueryBody {

  private final Select select;
  private final Optional<Query> first;
  private final Optional<Query> second;
  private final List<Identifier> attributeCols;
  private final Optional<RatioMetricExpression> ratioMetricExpr;
  private final Optional<LongLiteral> maxCombo;
  private final Optional<Expression> where;
  private final Optional<OrderBy> orderBy;
  private final Optional<String> limit;

  private static final LongLiteral DEFAULT_MAX_COMBO = new LongLiteral("3");
  private static final RatioMetricExpression DEFAULT_RATIO_METRIC_EXPRESSION =
      new RatioMetricExpression(new Identifier("global_ratio"),
          new AggregateExpression(new Aggregate("COUNT")));

  public DiffQuerySpecification(
      Select select,
      Optional<Query> first,
      Optional<Query> second,
      List<Identifier> attributeCols,
      Optional<RatioMetricExpression> ratioMetricExpr,
      Optional<LongLiteral> maxCombo,
      Optional<Expression> where,
      Optional<OrderBy> orderBy,
      Optional<String> limit) {
    this(Optional.empty(), select, first, second, attributeCols, ratioMetricExpr, maxCombo, where,
        orderBy, limit);
  }

  public DiffQuerySpecification(
      NodeLocation location,
      Select select,
      Optional<Query> first,
      Optional<Query> second,
      List<Identifier> attributeCols,
      Optional<RatioMetricExpression> ratioMetricExpr,
      Optional<LongLiteral> maxCombo,
      Optional<Expression> where,
      Optional<OrderBy> orderBy,
      Optional<String> limit) {
    this(Optional.of(location), select, first, second, attributeCols, ratioMetricExpr, maxCombo,
        where, orderBy, limit);
  }

  private DiffQuerySpecification(
      Optional<NodeLocation> location,
      Select select,
      Optional<Query> first,
      Optional<Query> second,
      List<Identifier> attributeCols,
      Optional<RatioMetricExpression> ratioMetricExpr,
      Optional<LongLiteral> maxCombo,
      Optional<Expression> where,
      Optional<OrderBy> orderBy,
      Optional<String> limit) {
    super(location);
    requireNonNull(select, "select is null");
    requireNonNull(first, "first is null");
    requireNonNull(second, "second is null");
    requireNonNull(attributeCols, "attributeCols is null");
    requireNonNull(ratioMetricExpr, "ratioMetricExpr is null");
    requireNonNull(maxCombo, "maxCombo is null");
    requireNonNull(where, "where is null");
    requireNonNull(orderBy, "orderBy is null");
    requireNonNull(limit, "limit is null");

    this.select = select;
    this.first = first;
    this.second = second;
    this.attributeCols = attributeCols;
    this.ratioMetricExpr = Optional.of(ratioMetricExpr.orElse(DEFAULT_RATIO_METRIC_EXPRESSION));
    this.maxCombo = Optional.of(maxCombo.orElse(DEFAULT_MAX_COMBO));
    this.where = where;
    this.orderBy = orderBy;
    this.limit = limit;
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
    nodes.add(ratioMetricExpr.get());
    nodes.add(new LongLiteral("" + maxCombo));
    where.ifPresent(nodes::add);
    orderBy.ifPresent(nodes::add);
    return nodes.build();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("select", select)
        .add("first", first)
        .add("second", second.orElse(null))
        .add("attributeCols", attributeCols)
        .add("ratioMetricExpr", ratioMetricExpr)
        .add("maxCombo", maxCombo)
        .add("where", where.orElse(null))
        .add("orderBy", orderBy)
        .add("limit", limit.orElse(null))
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
        Objects.equals(ratioMetricExpr, o.ratioMetricExpr) &&
        Objects.equals(maxCombo, o.maxCombo) &&
        Objects.equals(where, o.where) &&
        Objects.equals(orderBy, o.orderBy) &&
        Objects.equals(limit, o.limit);
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(select, first, second, attributeCols, ratioMetricExpr, maxCombo, where, orderBy,
            limit);
  }
}
