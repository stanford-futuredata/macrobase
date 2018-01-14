/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.stanford.futuredata.macrobase.sql.parser;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import edu.stanford.futuredata.macrobase.SqlBaseBaseVisitor;
import edu.stanford.futuredata.macrobase.SqlBaseLexer;
import edu.stanford.futuredata.macrobase.SqlBaseParser;
import edu.stanford.futuredata.macrobase.sql.tree.Aggregate;
import edu.stanford.futuredata.macrobase.sql.tree.AggregateExpression;
import edu.stanford.futuredata.macrobase.sql.tree.AliasedRelation;
import edu.stanford.futuredata.macrobase.sql.tree.AllColumns;
import edu.stanford.futuredata.macrobase.sql.tree.ArithmeticBinaryExpression;
import edu.stanford.futuredata.macrobase.sql.tree.ArithmeticUnaryExpression;
import edu.stanford.futuredata.macrobase.sql.tree.ArrayConstructor;
import edu.stanford.futuredata.macrobase.sql.tree.AtTimeZone;
import edu.stanford.futuredata.macrobase.sql.tree.BetweenPredicate;
import edu.stanford.futuredata.macrobase.sql.tree.BinaryLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.BindExpression;
import edu.stanford.futuredata.macrobase.sql.tree.BooleanLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.Cast;
import edu.stanford.futuredata.macrobase.sql.tree.CharLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.CoalesceExpression;
import edu.stanford.futuredata.macrobase.sql.tree.ColumnDefinition;
import edu.stanford.futuredata.macrobase.sql.tree.ComparisonExpression;
import edu.stanford.futuredata.macrobase.sql.tree.ComparisonExpressionType;
import edu.stanford.futuredata.macrobase.sql.tree.Cube;
import edu.stanford.futuredata.macrobase.sql.tree.DecimalLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.DelimiterClause;
import edu.stanford.futuredata.macrobase.sql.tree.DereferenceExpression;
import edu.stanford.futuredata.macrobase.sql.tree.DiffQuerySpecification;
import edu.stanford.futuredata.macrobase.sql.tree.DoubleLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.Except;
import edu.stanford.futuredata.macrobase.sql.tree.ExistsPredicate;
import edu.stanford.futuredata.macrobase.sql.tree.ExportClause;
import edu.stanford.futuredata.macrobase.sql.tree.Expression;
import edu.stanford.futuredata.macrobase.sql.tree.Extract;
import edu.stanford.futuredata.macrobase.sql.tree.FrameBound;
import edu.stanford.futuredata.macrobase.sql.tree.FunctionCall;
import edu.stanford.futuredata.macrobase.sql.tree.GenericLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.GroupBy;
import edu.stanford.futuredata.macrobase.sql.tree.GroupingElement;
import edu.stanford.futuredata.macrobase.sql.tree.GroupingOperation;
import edu.stanford.futuredata.macrobase.sql.tree.GroupingSets;
import edu.stanford.futuredata.macrobase.sql.tree.Identifier;
import edu.stanford.futuredata.macrobase.sql.tree.IfExpression;
import edu.stanford.futuredata.macrobase.sql.tree.ImportCsv;
import edu.stanford.futuredata.macrobase.sql.tree.InListExpression;
import edu.stanford.futuredata.macrobase.sql.tree.InPredicate;
import edu.stanford.futuredata.macrobase.sql.tree.IntLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.Intersect;
import edu.stanford.futuredata.macrobase.sql.tree.IntervalLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.IsNotNullPredicate;
import edu.stanford.futuredata.macrobase.sql.tree.IsNullPredicate;
import edu.stanford.futuredata.macrobase.sql.tree.Join;
import edu.stanford.futuredata.macrobase.sql.tree.JoinCriteria;
import edu.stanford.futuredata.macrobase.sql.tree.JoinOn;
import edu.stanford.futuredata.macrobase.sql.tree.JoinUsing;
import edu.stanford.futuredata.macrobase.sql.tree.LambdaArgumentDeclaration;
import edu.stanford.futuredata.macrobase.sql.tree.LambdaExpression;
import edu.stanford.futuredata.macrobase.sql.tree.LikePredicate;
import edu.stanford.futuredata.macrobase.sql.tree.LogicalBinaryExpression;
import edu.stanford.futuredata.macrobase.sql.tree.MinRatioExpression;
import edu.stanford.futuredata.macrobase.sql.tree.MinSupportExpression;
import edu.stanford.futuredata.macrobase.sql.tree.NaturalJoin;
import edu.stanford.futuredata.macrobase.sql.tree.Node;
import edu.stanford.futuredata.macrobase.sql.tree.NodeLocation;
import edu.stanford.futuredata.macrobase.sql.tree.NotExpression;
import edu.stanford.futuredata.macrobase.sql.tree.NullIfExpression;
import edu.stanford.futuredata.macrobase.sql.tree.NullLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.OrderBy;
import edu.stanford.futuredata.macrobase.sql.tree.Parameter;
import edu.stanford.futuredata.macrobase.sql.tree.QualifiedName;
import edu.stanford.futuredata.macrobase.sql.tree.QuantifiedComparisonExpression;
import edu.stanford.futuredata.macrobase.sql.tree.Query;
import edu.stanford.futuredata.macrobase.sql.tree.QueryBody;
import edu.stanford.futuredata.macrobase.sql.tree.QuerySpecification;
import edu.stanford.futuredata.macrobase.sql.tree.RatioMetricExpression;
import edu.stanford.futuredata.macrobase.sql.tree.Relation;
import edu.stanford.futuredata.macrobase.sql.tree.Rollup;
import edu.stanford.futuredata.macrobase.sql.tree.Row;
import edu.stanford.futuredata.macrobase.sql.tree.SampledRelation;
import edu.stanford.futuredata.macrobase.sql.tree.SearchedCaseExpression;
import edu.stanford.futuredata.macrobase.sql.tree.Select;
import edu.stanford.futuredata.macrobase.sql.tree.SelectItem;
import edu.stanford.futuredata.macrobase.sql.tree.SimpleCaseExpression;
import edu.stanford.futuredata.macrobase.sql.tree.SimpleGroupBy;
import edu.stanford.futuredata.macrobase.sql.tree.SingleColumn;
import edu.stanford.futuredata.macrobase.sql.tree.SortItem;
import edu.stanford.futuredata.macrobase.sql.tree.SplitQuery;
import edu.stanford.futuredata.macrobase.sql.tree.StringLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.SubqueryExpression;
import edu.stanford.futuredata.macrobase.sql.tree.SubscriptExpression;
import edu.stanford.futuredata.macrobase.sql.tree.Table;
import edu.stanford.futuredata.macrobase.sql.tree.TableSubquery;
import edu.stanford.futuredata.macrobase.sql.tree.TimeLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.TimestampLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.TryExpression;
import edu.stanford.futuredata.macrobase.sql.tree.Union;
import edu.stanford.futuredata.macrobase.sql.tree.Values;
import edu.stanford.futuredata.macrobase.sql.tree.WhenClause;
import edu.stanford.futuredata.macrobase.sql.tree.Window;
import edu.stanford.futuredata.macrobase.sql.tree.WindowFrame;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

class AstBuilder
    extends SqlBaseBaseVisitor<Node> {

    private int parameterPosition = 0;

    @Override
    public Node visitSingleStatement(SqlBaseParser.SingleStatementContext context) {
        return visit(context.statement());
    }

    @Override
    public Node visitSingleExpression(SqlBaseParser.SingleExpressionContext context) {
        return visit(context.expression());
    }

    // ********************** query expressions ********************

    @Override
    public Node visitQuery(SqlBaseParser.QueryContext context) {
        QueryBody term = (QueryBody) visit(context.queryTerm());

        if (term instanceof QuerySpecification) {
            // When we have a simple query specification
            // followed by order by limit, fold the order by and limit
            // clauses into the query specification (analyzer/planner
            // expects this structure to resolve references with respect
            // to columns defined in the query specification)
            QuerySpecification query = (QuerySpecification) term;

            return new Query(
                getLocation(context),
                new QuerySpecification(
                    getLocation(context),
                    query.getSelect(),
                    query.getFrom(),
                    query.getWhere(),
                    query.getGroupBy(),
                    query.getHaving(),
                    query.getOrderBy(),
                    query.getLimit(),
                    query.getExportExpr()));
        } else if (term instanceof DiffQuerySpecification) {

            DiffQuerySpecification diffQuery = (DiffQuerySpecification) term;
            return new Query(
                getLocation(context),
                new DiffQuerySpecification(
                    getLocation(context),
                    diffQuery.getSelect(),
                    diffQuery.getFirst(),
                    diffQuery.getSecond(),
                    diffQuery.getSplitQuery(),
                    diffQuery.getAttributeCols(),
                    Optional.of(diffQuery.getMinRatioExpression()),
                    Optional.of(diffQuery.getMinSupportExpression()),
                    Optional.of(diffQuery.getRatioMetricExpr()),
                    Optional.of(diffQuery.getMaxCombo()),
                    diffQuery.getWhere(),
                    diffQuery.getOrderBy(),
                    diffQuery.getLimit(),
                    diffQuery.getExportExpr()));
        }

        return new Query(
            getLocation(context),
            term);
    }

    // Import CSVs into SQL
    @Override
    public Node visitImportCsv(SqlBaseParser.ImportCsvContext context) {
        String filename = context.STRING().getText();
        filename = filename.substring(1, filename.length() - 1);
        // Remove single quotes from beginning and end of filename
        final List<ColumnDefinition> columns = visit(context.columnDefinition(),
            ColumnDefinition.class);
        return new ImportCsv(
            getLocation(context),
            filename,
            getQualifiedName(context.qualifiedName()),
            columns
        );
    }

    // Exporting queries to CSVs
    @Override
    public Node visitExportClause(SqlBaseParser.ExportClauseContext context) {
        // TODO: support custom escape character
        return new ExportClause(getLocation(context),
            visitIfPresent(context.delimiterClause(0), DelimiterClause.class),
            visitIfPresent(context.delimiterClause(1), DelimiterClause.class),
            unquote(getTextIfPresent(context.filename).get()));
    }

    @Override
    public Node visitDelimiterClause(SqlBaseParser.DelimiterClauseContext context) {
        return new DelimiterClause(getLocation(context), unquote(context.delimiter.getText()));
    }

    /***** Begin DIFF query ******/
    @Override
    public Node visitAggregate(SqlBaseParser.AggregateContext context) {
        return new Aggregate(getLocation(context), context.getText());
    }

    @Override
    public Node visitAggregateExpression(SqlBaseParser.AggregateExpressionContext context) {
        return new AggregateExpression(getLocation(context),
            (Aggregate) visit(context.aggregate()));
    }

    @Override
    public Node visitMinRatioExpression(SqlBaseParser.MinRatioExpressionContext context) {
        return new MinRatioExpression(getLocation(context),
            new DecimalLiteral(context.minRatio.getText()));
    }

    @Override
    public Node visitMinSupportExpression(SqlBaseParser.MinSupportExpressionContext context) {
        return new MinSupportExpression(getLocation(context),
            new DecimalLiteral(context.minSupport.getText()));
    }

    @Override
    public Node visitRatioMetricExpression(SqlBaseParser.RatioMetricExpressionContext context) {
        return new RatioMetricExpression(getLocation(context),
            (Identifier) visit(context.identifier()),
            (AggregateExpression) visit(context.aggregateExpression()));
    }

    @Override
    public Node visitSplitQuery(SqlBaseParser.SplitQueryContext context) {
        Identifier classifierName = (Identifier) visit(context.identifier());
        List<Expression> classifierArgs = visit(context.primaryExpression(), Expression.class);
        Optional<Relation> relation = visitIfPresent(context.relation(), Relation.class);
        Optional<TableSubquery> subquery = visitIfPresent(context.queryTerm(), TableSubquery.class);
        check(relation.isPresent() || subquery.isPresent(),
            "Either a relation or a subquery must be present in a SplitQuery", context);
        return new SplitQuery(classifierName, classifierArgs, relation, subquery);
    }

    @Override
    public Node visitDiffQuerySpecification(SqlBaseParser.DiffQuerySpecificationContext context) {
        Optional<SplitQuery> splitQuery = visitIfPresent(context.splitQuery(), SplitQuery.class);
        Optional<TableSubquery> first = Optional.empty();
        Optional<TableSubquery> second = Optional.empty();
        List<SelectItem> selectItems = visit(context.selectItem(), SelectItem.class);

        List<TableSubquery> subqueries = visit(context.queryTerm(), TableSubquery.class);
        check(subqueries.size() == 0 && splitQuery.isPresent()
                || subqueries.size() == 2 && !splitQuery
                .isPresent(),
            "At least one and at most two subqueries required for a DiffQuery", context);

        if (subqueries.size() == 2) {
            first = Optional.of(subqueries.get(0));
            second = Optional.of(subqueries.get(1));
        }

        Optional<MinRatioExpression> minRatioExpr = visitIfPresent(context.minRatioExpression(),
            MinRatioExpression.class);
        Optional<MinSupportExpression> minSupportExpr = visitIfPresent(
            context.minSupportExpression(),
            MinSupportExpression.class);

        Optional<RatioMetricExpression> ratioMetricExpr = visitIfPresent(
            context.ratioMetricExpression(), RatioMetricExpression.class);
        List<Identifier> attributeCols = visit(context.columnAliases().identifier(),
            Identifier.class);
        check(attributeCols.size() > 0, "At least one attribute must be specified", context);

        Optional<IntLiteral> maxCombo = getTextIfPresent(context.maxCombo).map(IntLiteral::new);

        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional
                .of(new OrderBy(getLocation(context.ORDER()),
                    visit(context.sortItem(), SortItem.class)));
        }

        Optional<ExportClause> exportExpr = visitIfPresent(context.exportClause(),
            ExportClause.class);

        return new DiffQuerySpecification(
            getLocation(context),
            new Select(getLocation(context.SELECT()), isDistinct(context.setQuantifier()),
                selectItems),
            first,
            second,
            splitQuery,
            attributeCols,
            minRatioExpr,
            minSupportExpr,
            ratioMetricExpr,
            maxCombo,
            visitIfPresent(context.where, Expression.class),
            orderBy,
            getTextIfPresent(context.limit),
            exportExpr);
    }

    /***** End DIFF query ******/

    @Override
    public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext context) {
        Optional<Relation> from = Optional.empty();
        List<SelectItem> selectItems = visit(context.selectItem(), SelectItem.class);

        List<Relation> relations = visit(context.relation(), Relation.class);
        if (!relations.isEmpty()) {
            // synthesize implicit join nodes
            Iterator<Relation> iterator = relations.iterator();
            Relation relation = iterator.next();

            while (iterator.hasNext()) {
                relation = new Join(getLocation(context), Join.Type.IMPLICIT, relation,
                    iterator.next(),
                    Optional.empty());
            }

            from = Optional.of(relation);
        }

        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional
                .of(new OrderBy(getLocation(context.ORDER()),
                    visit(context.sortItem(), SortItem.class)));
        }

        Optional<ExportClause> exportExpr = visitIfPresent(context.exportClause(),
            ExportClause.class);

        return new QuerySpecification(
            getLocation(context),
            new Select(getLocation(context.SELECT()), isDistinct(context.setQuantifier()),
                selectItems),
            from,
            visitIfPresent(context.where, Expression.class),
            visitIfPresent(context.groupBy(), GroupBy.class),
            visitIfPresent(context.having, Expression.class),
            orderBy,
            getTextIfPresent(context.limit),
            exportExpr);
    }

    @Override
    public Node visitGroupBy(SqlBaseParser.GroupByContext context) {
        return new GroupBy(getLocation(context), isDistinct(context.setQuantifier()),
            visit(context.groupingElement(), GroupingElement.class));
    }

    @Override
    public Node visitSingleGroupingSet(SqlBaseParser.SingleGroupingSetContext context) {
        return new SimpleGroupBy(getLocation(context),
            visit(context.groupingExpressions().expression(), Expression.class));
    }

    @Override
    public Node visitRollup(SqlBaseParser.RollupContext context) {
        return new Rollup(getLocation(context), context.qualifiedName().stream()
            .map(this::getQualifiedName)
            .collect(toList()));
    }

    @Override
    public Node visitCube(SqlBaseParser.CubeContext context) {
        return new Cube(getLocation(context), context.qualifiedName().stream()
            .map(this::getQualifiedName)
            .collect(toList()));
    }

    @Override
    public Node visitMultipleGroupingSets(SqlBaseParser.MultipleGroupingSetsContext context) {
        return new GroupingSets(getLocation(context), context.groupingSet().stream()
            .map(groupingSet -> groupingSet.qualifiedName().stream()
                .map(this::getQualifiedName)
                .collect(toList()))
            .collect(toList()));
    }

    @Override
    public Node visitSetOperation(SqlBaseParser.SetOperationContext context) {
        QueryBody left = (QueryBody) visit(context.left);
        QueryBody right = (QueryBody) visit(context.right);

        boolean distinct =
            context.setQuantifier() == null || context.setQuantifier().DISTINCT() != null;

        switch (context.operator.getType()) {
            case SqlBaseLexer.UNION:
                return new Union(getLocation(context.UNION()), ImmutableList.of(left, right),
                    distinct);
            case SqlBaseLexer.INTERSECT:
                return new Intersect(getLocation(context.INTERSECT()),
                    ImmutableList.of(left, right),
                    distinct);
            case SqlBaseLexer.EXCEPT:
                return new Except(getLocation(context.EXCEPT()), left, right, distinct);
        }

        throw new IllegalArgumentException(
            "Unsupported set operation: " + context.operator.getText());
    }

    @Override
    public Node visitSelectAll(SqlBaseParser.SelectAllContext context) {
        if (context.qualifiedName() != null) {
            return new AllColumns(getLocation(context), getQualifiedName(context.qualifiedName()));
        }

        return new AllColumns(getLocation(context));
    }

    @Override
    public Node visitSelectSingle(SqlBaseParser.SelectSingleContext context) {
        return new SingleColumn(
            getLocation(context),
            (Expression) visit(context.expression()),
            visitIfPresent(context.identifier(), Identifier.class));
    }

    @Override
    public Node visitTable(SqlBaseParser.TableContext context) {
        return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitSubquery(SqlBaseParser.SubqueryContext context) {
        return new TableSubquery(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitInlineTable(SqlBaseParser.InlineTableContext context) {
        return new Values(getLocation(context), visit(context.expression(), Expression.class));
    }

    // ***************** boolean expressions ******************

    @Override
    public Node visitLogicalNot(SqlBaseParser.LogicalNotContext context) {
        return new NotExpression(getLocation(context),
            (Expression) visit(context.booleanExpression()));
    }

    @Override
    public Node visitLogicalBinary(SqlBaseParser.LogicalBinaryContext context) {
        return new LogicalBinaryExpression(
            getLocation(context.operator),
            getLogicalBinaryOperator(context.operator),
            (Expression) visit(context.left),
            (Expression) visit(context.right));
    }

    // *************** from clause *****************

    @Override
    public Node visitJoinRelation(SqlBaseParser.JoinRelationContext context) {
        Relation left = (Relation) visit(context.left);
        Relation right;

        if (context.CROSS() != null) {
            right = (Relation) visit(context.right);
            return new Join(getLocation(context), Join.Type.CROSS, left, right, Optional.empty());
        }

        JoinCriteria criteria;
        if (context.NATURAL() != null) {
            right = (Relation) visit(context.right);
            criteria = new NaturalJoin();
        } else {
            right = (Relation) visit(context.rightRelation);
            if (context.joinCriteria().ON() != null) {
                criteria = new JoinOn(
                    (Expression) visit(context.joinCriteria().booleanExpression()));
            } else if (context.joinCriteria().USING() != null) {
                criteria = new JoinUsing(
                    visit(context.joinCriteria().identifier(), Identifier.class));
            } else {
                throw new IllegalArgumentException("Unsupported join criteria");
            }
        }

        Join.Type joinType;
        if (context.joinType().LEFT() != null) {
            joinType = Join.Type.LEFT;
        } else if (context.joinType().RIGHT() != null) {
            joinType = Join.Type.RIGHT;
        } else if (context.joinType().FULL() != null) {
            joinType = Join.Type.FULL;
        } else {
            joinType = Join.Type.INNER;
        }

        return new Join(getLocation(context), joinType, left, right, Optional.of(criteria));
    }

    @Override
    public Node visitSampledRelation(SqlBaseParser.SampledRelationContext context) {
        Relation child = (Relation) visit(context.aliasedRelation());

        if (context.TABLESAMPLE() == null) {
            return child;
        }

        return new SampledRelation(
            getLocation(context),
            child,
            getSamplingMethod((Token) context.sampleType().getChild(0).getPayload()),
            (Expression) visit(context.percentage));
    }

    @Override
    public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext context) {
        Relation child = (Relation) visit(context.relationPrimary());

        if (context.identifier() == null) {
            return child;
        }

        List<Identifier> aliases = null;
        if (context.columnAliases() != null) {
            aliases = visit(context.columnAliases().identifier(), Identifier.class);
        }

        return new AliasedRelation(getLocation(context), child,
            (Identifier) visit(context.identifier()), aliases);
    }

    @Override
    public Node visitTableName(SqlBaseParser.TableNameContext context) {
        return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitSubqueryRelation(SqlBaseParser.SubqueryRelationContext context) {
        return new TableSubquery(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitParenthesizedRelation(SqlBaseParser.ParenthesizedRelationContext context) {
        return visit(context.relation());
    }

    // ********************* predicates *******************

    @Override
    public Node visitPredicated(SqlBaseParser.PredicatedContext context) {
        if (context.predicate() != null) {
            return visit(context.predicate());
        }

        return visit(context.valueExpression);
    }

    @Override
    public Node visitComparison(SqlBaseParser.ComparisonContext context) {
        return new ComparisonExpression(
            getLocation(context.comparisonOperator()),
            getComparisonOperator(
                ((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
            (Expression) visit(context.value),
            (Expression) visit(context.right));
    }

    @Override
    public Node visitDistinctFrom(SqlBaseParser.DistinctFromContext context) {
        Expression expression = new ComparisonExpression(
            getLocation(context),
            ComparisonExpressionType.IS_DISTINCT_FROM,
            (Expression) visit(context.value),
            (Expression) visit(context.right));

        if (context.NOT() != null) {
            expression = new NotExpression(getLocation(context), expression);
        }

        return expression;
    }

    @Override
    public Node visitBetween(SqlBaseParser.BetweenContext context) {
        Expression expression = new BetweenPredicate(
            getLocation(context),
            (Expression) visit(context.value),
            (Expression) visit(context.lower),
            (Expression) visit(context.upper));

        if (context.NOT() != null) {
            expression = new NotExpression(getLocation(context), expression);
        }

        return expression;
    }

    @Override
    public Node visitNullPredicate(SqlBaseParser.NullPredicateContext context) {
        Expression child = (Expression) visit(context.value);

        if (context.NOT() == null) {
            return new IsNullPredicate(getLocation(context), child);
        }

        return new IsNotNullPredicate(getLocation(context), child);
    }

    @Override
    public Node visitLike(SqlBaseParser.LikeContext context) {
        Expression escape = null;
        if (context.escape != null) {
            escape = (Expression) visit(context.escape);
        }

        Expression result = new LikePredicate(getLocation(context),
            (Expression) visit(context.value),
            (Expression) visit(context.pattern), escape);

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitInList(SqlBaseParser.InListContext context) {
        Expression result = new InPredicate(
            getLocation(context),
            (Expression) visit(context.value),
            new InListExpression(getLocation(context),
                visit(context.expression(), Expression.class)));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitInSubquery(SqlBaseParser.InSubqueryContext context) {
        Expression result = new InPredicate(
            getLocation(context),
            (Expression) visit(context.value),
            new SubqueryExpression(getLocation(context), (Query) visit(context.query())));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitExists(SqlBaseParser.ExistsContext context) {
        return new ExistsPredicate(getLocation(context),
            new SubqueryExpression(getLocation(context), (Query) visit(context.query())));
    }

    @Override
    public Node visitQuantifiedComparison(SqlBaseParser.QuantifiedComparisonContext context) {
        return new QuantifiedComparisonExpression(
            getLocation(context.comparisonOperator()),
            getComparisonOperator(
                ((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
            getComparisonQuantifier(
                ((TerminalNode) context.comparisonQuantifier().getChild(0)).getSymbol()),
            (Expression) visit(context.value),
            new SubqueryExpression(getLocation(context.query()), (Query) visit(context.query())));
    }

    // ************** value expressions **************

    @Override
    public Node visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext context) {
        Expression child = (Expression) visit(context.valueExpression());

        switch (context.operator.getType()) {
            case SqlBaseLexer.MINUS:
                return ArithmeticUnaryExpression.negative(getLocation(context), child);
            case SqlBaseLexer.PLUS:
                return ArithmeticUnaryExpression.positive(getLocation(context), child);
            default:
                throw new UnsupportedOperationException(
                    "Unsupported sign: " + context.operator.getText());
        }
    }

    @Override
    public Node visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext context) {
        return new ArithmeticBinaryExpression(
            getLocation(context.operator),
            getArithmeticBinaryOperator(context.operator),
            (Expression) visit(context.left),
            (Expression) visit(context.right));
    }

    @Override
    public Node visitConcatenation(SqlBaseParser.ConcatenationContext context) {
        return new FunctionCall(
            getLocation(context.CONCAT()),
            QualifiedName.of("concat"), ImmutableList.of(
            (Expression) visit(context.left),
            (Expression) visit(context.right)));
    }

    @Override
    public Node visitAtTimeZone(SqlBaseParser.AtTimeZoneContext context) {
        return new AtTimeZone(
            getLocation(context.AT()),
            (Expression) visit(context.valueExpression()),
            (Expression) visit(context.timeZoneSpecifier()));
    }

    @Override
    public Node visitTimeZoneInterval(SqlBaseParser.TimeZoneIntervalContext context) {
        return visit(context.interval());
    }

    @Override
    public Node visitTimeZoneString(SqlBaseParser.TimeZoneStringContext context) {
        return visit(context.string());
    }

    // ********************* primary expressions **********************

    @Override
    public Node visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext context) {
        return visit(context.expression());
    }

    @Override
    public Node visitRowConstructor(SqlBaseParser.RowConstructorContext context) {
        return new Row(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitArrayConstructor(SqlBaseParser.ArrayConstructorContext context) {
        return new ArrayConstructor(getLocation(context),
            visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitCast(SqlBaseParser.CastContext context) {
        boolean isTryCast = context.TRY_CAST() != null;
        return new Cast(getLocation(context), (Expression) visit(context.expression()),
            getType(context.type()), isTryCast);
    }

    @Override
    public Node visitExtract(SqlBaseParser.ExtractContext context) {
        String fieldString = context.identifier().getText();
        Extract.Field field;
        try {
            field = Extract.Field.valueOf(fieldString.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw parseError("Invalid EXTRACT field: " + fieldString, context);
        }
        return new Extract(getLocation(context), (Expression) visit(context.valueExpression()),
            field);
    }

    @Override
    public Node visitPosition(SqlBaseParser.PositionContext context) {
        List<Expression> arguments = Lists
            .reverse(visit(context.valueExpression(), Expression.class));
        return new FunctionCall(getLocation(context), QualifiedName.of("strpos"), arguments);
    }

    @Override
    public Node visitNormalize(SqlBaseParser.NormalizeContext context) {
        Expression str = (Expression) visit(context.valueExpression());
        String normalForm = Optional.ofNullable(context.normalForm())
            .map(ParserRuleContext::getText)
            .orElse("NFC");
        return new FunctionCall(getLocation(context), QualifiedName.of("normalize"),
            ImmutableList.of(str, new StringLiteral(getLocation(context), normalForm)));
    }

    @Override
    public Node visitSubscript(SqlBaseParser.SubscriptContext context) {
        return new SubscriptExpression(getLocation(context), (Expression) visit(context.value),
            (Expression) visit(context.index));
    }

    @Override
    public Node visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext context) {
        return new SubqueryExpression(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitDereference(SqlBaseParser.DereferenceContext context) {
        return new DereferenceExpression(
            getLocation(context),
            (Expression) visit(context.base),
            (Identifier) visit(context.fieldName));
    }

    @Override
    public Node visitColumnReference(SqlBaseParser.ColumnReferenceContext context) {
        return visit(context.identifier());
    }

    @Override
    public Node visitSimpleCase(SqlBaseParser.SimpleCaseContext context) {
        return new SimpleCaseExpression(
            getLocation(context),
            (Expression) visit(context.valueExpression()),
            visit(context.whenClause(), WhenClause.class),
            visitIfPresent(context.elseExpression, Expression.class));
    }

    @Override
    public Node visitSearchedCase(SqlBaseParser.SearchedCaseContext context) {
        return new SearchedCaseExpression(
            getLocation(context),
            visit(context.whenClause(), WhenClause.class),
            visitIfPresent(context.elseExpression, Expression.class));
    }

    @Override
    public Node visitWhenClause(SqlBaseParser.WhenClauseContext context) {
        return new WhenClause(getLocation(context), (Expression) visit(context.condition),
            (Expression) visit(context.result));
    }

    @Override
    public Node visitFunctionCall(SqlBaseParser.FunctionCallContext context) {
        Optional<Expression> filter = visitIfPresent(context.filter(), Expression.class);
        Optional<Window> window = visitIfPresent(context.over(), Window.class);

        QualifiedName name = getQualifiedName(context.qualifiedName());

        boolean distinct = isDistinct(context.setQuantifier());

        if (name.toString().equalsIgnoreCase("if")) {
            check(context.expression().size() == 2 || context.expression().size() == 3,
                "Invalid number of arguments for 'if' function", context);
            check(!window.isPresent(), "OVER clause not valid for 'if' function", context);
            check(!distinct, "DISTINCT not valid for 'if' function", context);

            Expression elseExpression = null;
            if (context.expression().size() == 3) {
                elseExpression = (Expression) visit(context.expression(2));
            }

            return new IfExpression(
                getLocation(context),
                (Expression) visit(context.expression(0)),
                (Expression) visit(context.expression(1)),
                elseExpression);
        }

        if (name.toString().equalsIgnoreCase("nullif")) {
            check(context.expression().size() == 2,
                "Invalid number of arguments for 'nullif' function",
                context);
            check(!window.isPresent(), "OVER clause not valid for 'nullif' function", context);
            check(!distinct, "DISTINCT not valid for 'nullif' function", context);

            return new NullIfExpression(
                getLocation(context),
                (Expression) visit(context.expression(0)),
                (Expression) visit(context.expression(1)));
        }

        if (name.toString().equalsIgnoreCase("coalesce")) {
            check(context.expression().size() >= 2,
                "The 'coalesce' function must have at least two arguments", context);
            check(!window.isPresent(), "OVER clause not valid for 'coalesce' function", context);
            check(!distinct, "DISTINCT not valid for 'coalesce' function", context);

            return new CoalesceExpression(getLocation(context),
                visit(context.expression(), Expression.class));
        }

        if (name.toString().equalsIgnoreCase("try")) {
            check(context.expression().size() == 1,
                "The 'try' function must have exactly one argument",
                context);
            check(!window.isPresent(), "OVER clause not valid for 'try' function", context);
            check(!distinct, "DISTINCT not valid for 'try' function", context);

            return new TryExpression(getLocation(context),
                (Expression) visit(getOnlyElement(context.expression())));
        }

        if (name.toString().equalsIgnoreCase("$internal$bind")) {
            check(context.expression().size() >= 1,
                "The '$internal$bind' function must have at least one arguments", context);
            check(!window.isPresent(), "OVER clause not valid for '$internal$bind' function",
                context);
            check(!distinct, "DISTINCT not valid for '$internal$bind' function", context);

            int numValues = context.expression().size() - 1;
            List<Expression> arguments = context.expression().stream()
                .map(this::visit)
                .map(Expression.class::cast)
                .collect(toImmutableList());

            return new BindExpression(
                getLocation(context),
                arguments.subList(0, numValues),
                arguments.get(numValues));
        }

        return new FunctionCall(
            getLocation(context),
            getQualifiedName(context.qualifiedName()),
            window,
            filter,
            distinct,
            visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitLambda(SqlBaseParser.LambdaContext context) {
        List<LambdaArgumentDeclaration> arguments = visit(context.identifier(), Identifier.class)
            .stream()
            .map(LambdaArgumentDeclaration::new)
            .collect(toList());

        Expression body = (Expression) visit(context.expression());

        return new LambdaExpression(getLocation(context), arguments, body);
    }

    @Override
    public Node visitFilter(SqlBaseParser.FilterContext context) {
        return visit(context.booleanExpression());
    }

    @Override
    public Node visitOver(SqlBaseParser.OverContext context) {
        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional
                .of(new OrderBy(getLocation(context.ORDER()),
                    visit(context.sortItem(), SortItem.class)));
        }

        return new Window(
            getLocation(context),
            visit(context.partition, Expression.class),
            orderBy,
            visitIfPresent(context.windowFrame(), WindowFrame.class));
    }

    @Override
    public Node visitColumnDefinition(SqlBaseParser.ColumnDefinitionContext context) {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }
        return new ColumnDefinition(
            getLocation(context),
            (Identifier) visit(context.identifier()),
            getType(context.type()),
            comment);
    }

    @Override
    public Node visitSortItem(SqlBaseParser.SortItemContext context) {
        return new SortItem(
            getLocation(context),
            (Expression) visit(context.expression()),
            Optional.ofNullable(context.ordering)
                .map(AstBuilder::getOrderingType)
                .orElse(SortItem.Ordering.ASCENDING),
            Optional.ofNullable(context.nullOrdering)
                .map(AstBuilder::getNullOrderingType)
                .orElse(SortItem.NullOrdering.UNDEFINED));
    }

    @Override
    public Node visitWindowFrame(SqlBaseParser.WindowFrameContext context) {
        return new WindowFrame(
            getLocation(context),
            getFrameType(context.frameType),
            (FrameBound) visit(context.start),
            visitIfPresent(context.end, FrameBound.class));
    }

    @Override
    public Node visitUnboundedFrame(SqlBaseParser.UnboundedFrameContext context) {
        return new FrameBound(getLocation(context), getUnboundedFrameBoundType(context.boundType));
    }

    @Override
    public Node visitBoundedFrame(SqlBaseParser.BoundedFrameContext context) {
        return new FrameBound(getLocation(context), getBoundedFrameBoundType(context.boundType),
            (Expression) visit(context.expression()));
    }

    @Override
    public Node visitCurrentRowBound(SqlBaseParser.CurrentRowBoundContext context) {
        return new FrameBound(getLocation(context), FrameBound.Type.CURRENT_ROW);
    }

    @Override
    public Node visitGroupingOperation(SqlBaseParser.GroupingOperationContext context) {
        List<QualifiedName> arguments = context.qualifiedName().stream()
            .map(this::getQualifiedName)
            .collect(toList());

        return new GroupingOperation(Optional.of(getLocation(context)), arguments);
    }

    @Override
    public Node visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext context) {
        return new Identifier(getLocation(context), context.getText(), false);
    }

    // ************** literals **************

    @Override
    public Node visitNullLiteral(SqlBaseParser.NullLiteralContext context) {
        return new NullLiteral(getLocation(context));
    }

    @Override
    public Node visitBasicStringLiteral(SqlBaseParser.BasicStringLiteralContext context) {
        return new StringLiteral(getLocation(context), unquote(context.STRING().getText()));
    }

    @Override
    public Node visitUnicodeStringLiteral(SqlBaseParser.UnicodeStringLiteralContext context) {
        return new StringLiteral(getLocation(context), decodeUnicodeLiteral(context));
    }

    @Override
    public Node visitBinaryLiteral(SqlBaseParser.BinaryLiteralContext context) {
        String raw = context.BINARY_LITERAL().getText();
        return new BinaryLiteral(getLocation(context), unquote(raw.substring(1)));
    }

    @Override
    public Node visitTypeConstructor(SqlBaseParser.TypeConstructorContext context) {
        String value = ((StringLiteral) visit(context.string())).getValue();

        if (context.DOUBLE_PRECISION() != null) {
            // TODO: Temporary hack that should be removed with new planner.
            return new GenericLiteral(getLocation(context), "DOUBLE", value);
        }

        String type = context.identifier().getText();
        if (type.equalsIgnoreCase("time")) {
            return new TimeLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("timestamp")) {
            return new TimestampLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("decimal")) {
            return new DecimalLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("char")) {
            return new CharLiteral(getLocation(context), value);
        }

        return new GenericLiteral(getLocation(context), type, value);
    }

    @Override
    public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext context) {
        return new IntLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext context) {
        return new DoubleLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitBooleanValue(SqlBaseParser.BooleanValueContext context) {
        return new BooleanLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitInterval(SqlBaseParser.IntervalContext context) {
        return new IntervalLiteral(
            getLocation(context),
            ((StringLiteral) visit(context.string())).getValue(),
            Optional.ofNullable(context.sign)
                .map(AstBuilder::getIntervalSign)
                .orElse(IntervalLiteral.Sign.POSITIVE),
            getIntervalFieldType((Token) context.from.getChild(0).getPayload()),
            Optional.ofNullable(context.to)
                .map((x) -> x.getChild(0).getPayload())
                .map(Token.class::cast)
                .map(AstBuilder::getIntervalFieldType));
    }

    @Override
    public Node visitParameter(SqlBaseParser.ParameterContext context) {
        Parameter parameter = new Parameter(getLocation(context), parameterPosition);
        parameterPosition++;
        return parameter;
    }

    // ***************** helpers *****************

    @Override
    protected Node defaultResult() {
        return null;
    }

    @Override
    protected Node aggregateResult(Node aggregate, Node nextResult) {
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }

        if (aggregate == null) {
            return nextResult;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    private enum UnicodeDecodeState {
        EMPTY,
        ESCAPED,
        UNICODE_SEQUENCE
    }

    private static String decodeUnicodeLiteral(SqlBaseParser.UnicodeStringLiteralContext context) {
        char escape;
        if (context.UESCAPE() != null) {
            String escapeString = unquote(context.STRING().getText());
            check(!escapeString.isEmpty(), "Empty Unicode escape character", context);
            check(escapeString.length() == 1, "Invalid Unicode escape character: " + escapeString,
                context);
            escape = escapeString.charAt(0);
            check(isValidUnicodeEscape(escape), "Invalid Unicode escape character: " + escapeString,
                context);
        } else {
            escape = '\\';
        }

        String rawContent = unquote(context.UNICODE_STRING().getText().substring(2));
        StringBuilder unicodeStringBuilder = new StringBuilder();
        StringBuilder escapedCharacterBuilder = new StringBuilder();
        int charactersNeeded = 0;
        UnicodeDecodeState state = UnicodeDecodeState.EMPTY;
        for (int i = 0; i < rawContent.length(); i++) {
            char ch = rawContent.charAt(i);
            switch (state) {
                case EMPTY:
                    if (ch == escape) {
                        state = UnicodeDecodeState.ESCAPED;
                    } else {
                        unicodeStringBuilder.append(ch);
                    }
                    break;
                case ESCAPED:
                    if (ch == escape) {
                        unicodeStringBuilder.append(escape);
                        state = UnicodeDecodeState.EMPTY;
                    } else if (ch == '+') {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        charactersNeeded = 6;
                    } else if (isHexDigit(ch)) {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        charactersNeeded = 4;
                        escapedCharacterBuilder.append(ch);
                    } else {
                        throw parseError("Invalid hexadecimal digit: " + ch, context);
                    }
                    break;
                case UNICODE_SEQUENCE:
                    check(isHexDigit(ch),
                        "Incomplete escape sequence: " + escapedCharacterBuilder.toString(),
                        context);
                    escapedCharacterBuilder.append(ch);
                    if (charactersNeeded == escapedCharacterBuilder.length()) {
                        String currentEscapedCode = escapedCharacterBuilder.toString();
                        escapedCharacterBuilder.setLength(0);
                        int codePoint = Integer.parseInt(currentEscapedCode, 16);
                        check(Character.isValidCodePoint(codePoint),
                            "Invalid escaped character: " + currentEscapedCode, context);
                        if (Character.isSupplementaryCodePoint(codePoint)) {
                            unicodeStringBuilder.appendCodePoint(codePoint);
                        } else {
                            char currentCodePoint = (char) codePoint;
                            check(!Character.isSurrogate(currentCodePoint), format(
                                "Invalid escaped character: %s. Escaped character is a surrogate. Use '\\+123456' instead.",
                                currentEscapedCode), context);
                            unicodeStringBuilder.append(currentCodePoint);
                        }
                        state = UnicodeDecodeState.EMPTY;
                        charactersNeeded = -1;
                    } else {
                        check(charactersNeeded > escapedCharacterBuilder.length(),
                            "Unexpected escape sequence length: " + escapedCharacterBuilder
                                .length(), context);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        check(state == UnicodeDecodeState.EMPTY,
            "Incomplete escape sequence: " + escapedCharacterBuilder.toString(), context);
        return unicodeStringBuilder.toString();
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
        return Optional.ofNullable(context)
            .map(this::visit)
            .map(clazz::cast);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
            .map(this::visit)
            .map(clazz::cast)
            .collect(toList());
    }

    private static String unquote(String value) {
        return value.substring(1, value.length() - 1)
            .replace("''", "'").replace("\"\"", "\"");
    }

    private QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context) {
        List<String> parts = visit(context.identifier(), Identifier.class).stream()
            .map(Identifier::getValue) // TODO: preserve quotedness
            .collect(Collectors.toList());

        return QualifiedName.of(parts);
    }

    private static boolean isDistinct(SqlBaseParser.SetQuantifierContext setQuantifier) {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    private static boolean isHexDigit(char c) {
        return ((c >= '0') && (c <= '9')) ||
            ((c >= 'A') && (c <= 'F')) ||
            ((c >= 'a') && (c <= 'f'));
    }

    private static boolean isValidUnicodeEscape(char c) {
        return c < 0x7F && c > 0x20 && !isHexDigit(c) && c != '"' && c != '+' && c != '\'';
    }

    private static Optional<String> getTextIfPresent(Token token) {
        return Optional.ofNullable(token)
            .map(Token::getText);
    }

    private static ArithmeticBinaryExpression.Type getArithmeticBinaryOperator(Token operator) {
        switch (operator.getType()) {
            case SqlBaseLexer.PLUS:
                return ArithmeticBinaryExpression.Type.ADD;
            case SqlBaseLexer.MINUS:
                return ArithmeticBinaryExpression.Type.SUBTRACT;
            case SqlBaseLexer.ASTERISK:
                return ArithmeticBinaryExpression.Type.MULTIPLY;
            case SqlBaseLexer.SLASH:
                return ArithmeticBinaryExpression.Type.DIVIDE;
            case SqlBaseLexer.PERCENT:
                return ArithmeticBinaryExpression.Type.MODULUS;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    private static ComparisonExpressionType getComparisonOperator(Token symbol) {
        switch (symbol.getType()) {
            case SqlBaseLexer.EQ:
                return ComparisonExpressionType.EQUAL;
            case SqlBaseLexer.NEQ:
                return ComparisonExpressionType.NOT_EQUAL;
            case SqlBaseLexer.LT:
                return ComparisonExpressionType.LESS_THAN;
            case SqlBaseLexer.LTE:
                return ComparisonExpressionType.LESS_THAN_OR_EQUAL;
            case SqlBaseLexer.GT:
                return ComparisonExpressionType.GREATER_THAN;
            case SqlBaseLexer.GTE:
                return ComparisonExpressionType.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    private static IntervalLiteral.IntervalField getIntervalFieldType(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.YEAR:
                return IntervalLiteral.IntervalField.YEAR;
            case SqlBaseLexer.MONTH:
                return IntervalLiteral.IntervalField.MONTH;
            case SqlBaseLexer.DAY:
                return IntervalLiteral.IntervalField.DAY;
            case SqlBaseLexer.HOUR:
                return IntervalLiteral.IntervalField.HOUR;
            case SqlBaseLexer.MINUTE:
                return IntervalLiteral.IntervalField.MINUTE;
            case SqlBaseLexer.SECOND:
                return IntervalLiteral.IntervalField.SECOND;
        }

        throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
    }

    private static IntervalLiteral.Sign getIntervalSign(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.MINUS:
                return IntervalLiteral.Sign.NEGATIVE;
            case SqlBaseLexer.PLUS:
                return IntervalLiteral.Sign.POSITIVE;
        }

        throw new IllegalArgumentException("Unsupported sign: " + token.getText());
    }

    private static WindowFrame.Type getFrameType(Token type) {
        switch (type.getType()) {
            case SqlBaseLexer.RANGE:
                return WindowFrame.Type.RANGE;
            case SqlBaseLexer.ROWS:
                return WindowFrame.Type.ROWS;
        }

        throw new IllegalArgumentException("Unsupported frame type: " + type.getText());
    }

    private static FrameBound.Type getBoundedFrameBoundType(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.PRECEDING:
                return FrameBound.Type.PRECEDING;
            case SqlBaseLexer.FOLLOWING:
                return FrameBound.Type.FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    private static FrameBound.Type getUnboundedFrameBoundType(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.PRECEDING:
                return FrameBound.Type.UNBOUNDED_PRECEDING;
            case SqlBaseLexer.FOLLOWING:
                return FrameBound.Type.UNBOUNDED_FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    private static SampledRelation.Type getSamplingMethod(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.BERNOULLI:
                return SampledRelation.Type.BERNOULLI;
            case SqlBaseLexer.SYSTEM:
                return SampledRelation.Type.SYSTEM;
        }

        throw new IllegalArgumentException("Unsupported sampling method: " + token.getText());
    }

    private static LogicalBinaryExpression.Type getLogicalBinaryOperator(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.AND:
                return LogicalBinaryExpression.Type.AND;
            case SqlBaseLexer.OR:
                return LogicalBinaryExpression.Type.OR;
        }

        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }

    private static SortItem.NullOrdering getNullOrderingType(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.FIRST:
                return SortItem.NullOrdering.FIRST;
            case SqlBaseLexer.LAST:
                return SortItem.NullOrdering.LAST;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static SortItem.Ordering getOrderingType(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.ASC:
                return SortItem.Ordering.ASCENDING;
            case SqlBaseLexer.DESC:
                return SortItem.Ordering.DESCENDING;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static QuantifiedComparisonExpression.Quantifier getComparisonQuantifier(Token symbol) {
        switch (symbol.getType()) {
            case SqlBaseLexer.ALL:
                return QuantifiedComparisonExpression.Quantifier.ALL;
            case SqlBaseLexer.ANY:
                return QuantifiedComparisonExpression.Quantifier.ANY;
            case SqlBaseLexer.SOME:
                return QuantifiedComparisonExpression.Quantifier.SOME;
        }

        throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
    }

    private String getType(SqlBaseParser.TypeContext type) {
        if (type.baseType() != null) {
            String signature = type.baseType().getText();
            if (type.baseType().DOUBLE_PRECISION() != null) {
                // TODO: Temporary hack that should be removed with new planner.
                signature = "DOUBLE";
            }
            if (!type.typeParameter().isEmpty()) {
                String typeParameterSignature = type
                    .typeParameter()
                    .stream()
                    .map(this::typeParameterToString)
                    .collect(Collectors.joining(","));
                signature += "(" + typeParameterSignature + ")";
            }
            return signature;
        }

        if (type.ARRAY() != null) {
            return "ARRAY(" + getType(type.type(0)) + ")";
        }

        if (type.MAP() != null) {
            return "MAP(" + getType(type.type(0)) + "," + getType(type.type(1)) + ")";
        }

        if (type.ROW() != null) {
            StringBuilder builder = new StringBuilder("(");
            for (int i = 0; i < type.identifier().size(); i++) {
                if (i != 0) {
                    builder.append(",");
                }
                builder.append(visit(type.identifier(i)))
                    .append(" ")
                    .append(getType(type.type(i)));
            }
            builder.append(")");
            return "ROW" + builder.toString();
        }

        throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
    }

    private String typeParameterToString(SqlBaseParser.TypeParameterContext typeParameter) {
        if (typeParameter.INTEGER_VALUE() != null) {
            return typeParameter.INTEGER_VALUE().toString();
        }
        if (typeParameter.type() != null) {
            return getType(typeParameter.type());
        }
        throw new IllegalArgumentException("Unsupported typeParameter: " + typeParameter.getText());
    }

    private static void check(boolean condition, String message, ParserRuleContext context) {
        if (!condition) {
            throw parseError(message, context);
        }
    }

    private static NodeLocation getLocation(TerminalNode terminalNode) {
        requireNonNull(terminalNode, "terminalNode is null");
        return getLocation(terminalNode.getSymbol());
    }

    private static NodeLocation getLocation(ParserRuleContext parserRuleContext) {
        requireNonNull(parserRuleContext, "parserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    private static NodeLocation getLocation(Token token) {
        requireNonNull(token, "token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine());
    }

    private static ParsingException parseError(String message, ParserRuleContext context) {
        return new ParsingException(message, null, context.getStart().getLine(),
            context.getStart().getCharPositionInLine());
    }
}
