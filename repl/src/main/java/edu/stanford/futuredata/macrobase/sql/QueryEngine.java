package edu.stanford.futuredata.macrobase.sql;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.base.Joiner;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.sql.tree.ComparisonExpression;
import edu.stanford.futuredata.macrobase.sql.tree.ComparisonExpressionType;
import edu.stanford.futuredata.macrobase.sql.tree.DiffQuerySpecification;
import edu.stanford.futuredata.macrobase.sql.tree.DoubleLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.Expression;
import edu.stanford.futuredata.macrobase.sql.tree.Identifier;
import edu.stanford.futuredata.macrobase.sql.tree.ImportCsv;
import edu.stanford.futuredata.macrobase.sql.tree.Literal;
import edu.stanford.futuredata.macrobase.sql.tree.OrderBy;
import edu.stanford.futuredata.macrobase.sql.tree.Query;
import edu.stanford.futuredata.macrobase.sql.tree.QueryBody;
import edu.stanford.futuredata.macrobase.sql.tree.QuerySpecification;
import edu.stanford.futuredata.macrobase.sql.tree.Select;
import edu.stanford.futuredata.macrobase.sql.tree.SelectItem;
import edu.stanford.futuredata.macrobase.sql.tree.SortItem;
import edu.stanford.futuredata.macrobase.sql.tree.SortItem.Ordering;
import edu.stanford.futuredata.macrobase.sql.tree.StringLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.Table;
import edu.stanford.futuredata.macrobase.sql.tree.TableSubquery;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import edu.stanford.futuredata.macrobase.util.MacrobaseSQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.DoublePredicate;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryEngine {

  private static final Logger log = LoggerFactory.getLogger(MacrobaseSQLRepl.class);

  private final Map<String, DataFrame> tablesInMemory;

  public QueryEngine() {
    tablesInMemory = new HashMap<>();
  }

  public DataFrame importTableFromCsv(ImportCsv importStatement) {
    final String filename = importStatement.getFilename();
    final String tableName = importStatement.getTableName().toString();
    final Map<String, ColType> schema = importStatement.getSchema();
    try {
      DataFrame df = new CSVDataFrameLoader(filename).setColumnTypes(schema).load();
      tablesInMemory.put(tableName, df);
      return df;
    } catch (Exception e) {
      // TODO: better error message for when file is not found, and better alternative return value
      e.printStackTrace();
      return new DataFrame();
    }
  }

  public DataFrame executeQuery(Query query) throws MacrobaseException {
    QueryBody qBody = query.getQueryBody();
    if (qBody instanceof QuerySpecification) {
      QuerySpecification querySpec = (QuerySpecification) qBody;
      log.debug(querySpec.toString());
      return executeQuerySpec(querySpec);

    } else if (qBody instanceof DiffQuerySpecification) {
      DiffQuerySpecification diffQuery = (DiffQuerySpecification) qBody;
      log.debug(diffQuery.toString());
      return executeDiffQuerySpec(diffQuery);
    } else {
      return new DataFrame(); // TODO: think of better alternative
    }
  }

  private DataFrame executeDiffQuerySpec(final DiffQuerySpecification diffQuery)
      throws MacrobaseException {
    assert (diffQuery.getSecond().isPresent()); // TODO: support single DataFrame queries
    // Extract parameters for Diff query
    final Query first = diffQuery.getFirst().get();
    final Query second = diffQuery.getSecond().get();
    final List<String> explainCols = diffQuery.getAttributeCols().stream().map(Identifier::toString)
        .collect(toImmutableList());
    final String ratioMetricStr = diffQuery.getRatioMetricExpr().getFuncName().toString();
    final long order = diffQuery.getMaxCombo().get().getValue();

    // execute subqueries
    final DataFrame firstDf = executeQuery(((TableSubquery) first.getQueryBody()).getQuery());
    final DataFrame secondDf = executeQuery(((TableSubquery) second.getQueryBody()).getQuery());
    if (!firstDf.getSchema().hasColumns(explainCols) || !secondDf.getSchema()
        .hasColumns(explainCols)) {
      throw new MacrobaseSQLException(
          "ON " + Joiner.on(", ").join(explainCols) + " not present in either"
              + " outlier or inlier subquery");
    }
    // execute diff
    DataFrame df = Diff.diff(firstDf, secondDf, explainCols, ratioMetricStr, (int) order);
    return evaluateSQLClauses(diffQuery, df);
  }

  /**
   * Evaluate standard SQL clauses: SELECT, WHERE, ORDER BY, and LIMIT. TODO: support GROUP BY and
   * HAVING clauses
   *
   * @param query the query that contains the clauses
   * @param df the DataFrame to apply these clauses to
   * @return a new DataFrame, the result of applying all these clauses
   */
  private DataFrame evaluateSQLClauses(final QueryBody query, final DataFrame df)
      throws MacrobaseSQLException {
    // TODO: we need to figure out a smarter ordering of these. For example,
    // if we have an ORDER BY, we don't need to sort columns that are never going to be in the
    // final output (i.e. the ones not in the SELECT). Basically, we need to do two passes of
    // SELECTs: one with all original projections + the columns in the WHERE clauses and ORDER BY
    // clauses, and then a second with just the original projections. That should be correct
    // and give us better performance.
    DataFrame resultDf = evaluateWhereClause(df, query.getWhere());
    resultDf = evaluateOrderByClause(resultDf, query.getOrderBy());
    resultDf = evaluateLimitClause(resultDf, query.getLimit());
    return evaluateSelectClause(resultDf, query.getSelect());
  }

  /**
   * Evaluate ORDER BY clause. For now, we only support sorting by a single column.
   */
  private DataFrame evaluateOrderByClause(DataFrame df, Optional<OrderBy> orderByOpt) {
    if (!orderByOpt.isPresent()) {
      return df;
    }
    final OrderBy orderBy = orderByOpt.get();
    // For now, we only support sorting by a single column
    // TODO: support multi-column sort
    final SortItem sortItem = orderBy.getSortItems().get(0);
    final String sortCol = ((Identifier) sortItem.getSortKey()).getValue();
    return df.orderBy(sortCol, sortItem.getOrdering() == Ordering.ASCENDING);
  }

  /**
   * Execute a standard SQL query. For now, we ignore GROUP BY, HAVING, and JOIN clauses
   *
   * @param query The QuerySpecification object for the SQL query
   * @return A DataFrame containing the results of the SQL query
   */
  private DataFrame executeQuerySpec(final QuerySpecification query) throws MacrobaseSQLException {
    Table table = (Table) query.getFrom().get();
    final String tableName = table.getName().toString();
    if (!tablesInMemory.containsKey(tableName)) {
      throw new MacrobaseSQLException("Table " + tableName + " does not exist");
    }
    final DataFrame df = tablesInMemory.get(tableName);
    return evaluateSQLClauses(query, df);
  }

  /**
   * Evaluate Select clause of SQL query. If the clause is 'SELECT *' the same DataFrame is returned
   * unchanged. TODO: add support for DISTINCT queries
   *
   * @param df The DataFrame to apply the Select clause on
   * @param select The Select clause to evaluate
   * @return A new DataFrame with the result of the Select clause applied
   */
  private DataFrame evaluateSelectClause(DataFrame df, Select select) {
    final List<String> projections = select.getSelectItems().stream()
        .map(SelectItem::toString)
        .collect(toImmutableList());
    if (projections.size() == 1 && projections.get(0).equalsIgnoreCase("*")) {
      return df; // SELECT * -> relation is unchanged
    }
    return df.project(projections);
  }

  /**
   * Evaluate LIMIT clause of SQL query, return the top n rows of the DataFrame, where `n' is
   * specified in "LIMIT n"
   *
   * @param df The DataFrame to apply the LIMIT clause on
   * @param limitStr The number of rows (either an integer or "ALL") as a String in the LIMIT
   * clause
   * @return A new DataFrame with the result of the LIMIT clause applied
   */
  private DataFrame evaluateLimitClause(final DataFrame df, final Optional<String> limitStr) {
    if (limitStr.isPresent()) {
      try {
        return df.limit(Integer.parseInt(limitStr.get()));
      } catch (NumberFormatException e) {
        // LIMIT ALL, catch NumberFormatException and do nothing
        return df;
      }
    }
    return df;
  }

  /**
   * Evaluate Where clause of SQL query
   *
   * @param df the DataFrame to filter
   * @param whereClauseOpt An Optional Where clause (of type Expression) to evaluate for each row
   * in <tt>df</tt>
   * @return A new DataFrame that contains the rows for which @whereClause evaluates to true. If
   * <tt>whereClauseOpt</tt> is not Present, we return <tt>df</tt>
   */
  private DataFrame evaluateWhereClause(final DataFrame df,
      final Optional<Expression> whereClauseOpt) throws MacrobaseSQLException {
    if (!whereClauseOpt.isPresent()) {
      return df;
    }
    final Expression whereClause = whereClauseOpt.get();
    if (whereClause instanceof ComparisonExpression) {
      final ComparisonExpression compareExpr = (ComparisonExpression) whereClause;
      final Expression left = compareExpr.getLeft();
      final Expression right = compareExpr.getRight();
      final ComparisonExpressionType type = compareExpr.getType();

      if (left instanceof Literal && right instanceof Literal) {
        final boolean val = left.equals(right);
        final boolean[] mask = new boolean[df.getNumRows()];
        Arrays.fill(mask, val);
        return df.filter(mask);
      } else if (left instanceof Literal && right instanceof Identifier) {
        return evaluatePredicate(df, (Literal) left, (Identifier) right, type);
      } else if (right instanceof Literal && left instanceof Identifier) {
        return evaluatePredicate(df, (Literal) right, (Identifier) left, type);
      }
    } else {
      throw new MacrobaseSQLException("Only comparison expressions are supported");
    }
    return df;
  }

  // Helper methods for evaluating Where clauses
  private DataFrame evaluatePredicate(final DataFrame df, final Literal literal,
      final Identifier identifier,
      final ComparisonExpressionType compExprType) throws MacrobaseSQLException {
    final String colName = identifier.getValue();
    final int colIndex = df.getSchema().getColumnIndex(colName);
    final ColType colType = df.getSchema().getColumnType(colIndex);

    if (colType == ColType.DOUBLE) {
      if (!(literal instanceof DoubleLiteral)) {
        throw new MacrobaseSQLException(
            "Column " + colName + " has type " + colType + ", but " + literal
                + " is not a DoubleLiteral");
      }

      return df.filter(colIndex,
          generateLambdaForPredicate(((DoubleLiteral) literal).getValue(), compExprType));
    } else {
      // colType == ColType.STRING
      if (!(literal instanceof StringLiteral)) {
        throw new MacrobaseSQLException(
            "Column " + colName + " has type " + colType + ", but " + literal
                + " is not StringLiteral");
      }

      return df.filter(colIndex,
          generateLambdaForPredicate(((StringLiteral) literal).getValue(), compExprType));
    }
  }

  private DoublePredicate generateLambdaForPredicate(double y,
      ComparisonExpressionType compareExprType) throws MacrobaseSQLException {
    switch (compareExprType) {
      case EQUAL:
        return (x) -> x == y;
      case NOT_EQUAL:
      case IS_DISTINCT_FROM:
        // IS DISTINCT FROM is true when x and y have different values or
        // if one of them is NULL and the other isn't.
        // x and y can never be NULL here, so it's the same as NOT_EQUAL
        return (x) -> x != y;
      case LESS_THAN:
        return (x) -> x < y;
      case LESS_THAN_OR_EQUAL:
        return (x) -> x <= y;
      case GREATER_THAN:
        return (x) -> x > y;
      case GREATER_THAN_OR_EQUAL:
        return (x) -> x >= y;
      default:
        throw new MacrobaseSQLException(compareExprType + " is not supported");
    }
  }

  private Predicate<Object> generateLambdaForPredicate(final String y,
      final ComparisonExpressionType compareExprType) throws MacrobaseSQLException {
    switch (compareExprType) {
      case EQUAL:
        return (x) -> Objects.equals(x, y);
      case NOT_EQUAL:
      case IS_DISTINCT_FROM:
        // IS DISTINCT FROM is true when x and y have different values or
        // if one of them is NULL and the other isn't
        return (x) -> !Objects.equals(x, y);
      default:
        throw new MacrobaseSQLException(compareExprType + " is not supported");
    }
  }
}
