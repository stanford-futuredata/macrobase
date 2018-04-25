package edu.stanford.futuredata.macrobase.sql;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import edu.stanford.futuredata.macrobase.analysis.MBFunction;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLOutlierSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.util.ModBitSet;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.sql.tree.AllColumns;
import edu.stanford.futuredata.macrobase.sql.tree.ComparisonExpression;
import edu.stanford.futuredata.macrobase.sql.tree.ComparisonExpressionType;
import edu.stanford.futuredata.macrobase.sql.tree.DiffQuerySpecification;
import edu.stanford.futuredata.macrobase.sql.tree.DoubleLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.Expression;
import edu.stanford.futuredata.macrobase.sql.tree.FunctionCall;
import edu.stanford.futuredata.macrobase.sql.tree.Identifier;
import edu.stanford.futuredata.macrobase.sql.tree.ImportCsv;
import edu.stanford.futuredata.macrobase.sql.tree.Literal;
import edu.stanford.futuredata.macrobase.sql.tree.LogicalBinaryExpression;
import edu.stanford.futuredata.macrobase.sql.tree.LogicalBinaryExpression.Type;
import edu.stanford.futuredata.macrobase.sql.tree.NotExpression;
import edu.stanford.futuredata.macrobase.sql.tree.NullLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.OrderBy;
import edu.stanford.futuredata.macrobase.sql.tree.QueryBody;
import edu.stanford.futuredata.macrobase.sql.tree.QuerySpecification;
import edu.stanford.futuredata.macrobase.sql.tree.Relation;
import edu.stanford.futuredata.macrobase.sql.tree.Select;
import edu.stanford.futuredata.macrobase.sql.tree.SelectItem;
import edu.stanford.futuredata.macrobase.sql.tree.SingleColumn;
import edu.stanford.futuredata.macrobase.sql.tree.SortItem;
import edu.stanford.futuredata.macrobase.sql.tree.SortItem.Ordering;
import edu.stanford.futuredata.macrobase.sql.tree.SplitQuery;
import edu.stanford.futuredata.macrobase.sql.tree.StringLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.Table;
import edu.stanford.futuredata.macrobase.sql.tree.TableSubquery;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import edu.stanford.futuredata.macrobase.util.MacroBaseSQLException;

import java.util.*;
import java.util.function.DoublePredicate;
import java.util.function.Predicate;
import java.util.stream.DoubleStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueryEngine {

    private static final Logger log = LoggerFactory.getLogger(QueryEngine.class.getSimpleName());

    private final Map<String, DataFrame> tablesInMemory;
    private final int numThreads;

    QueryEngine() {
        tablesInMemory = new HashMap<>();
        numThreads = 1; // TODO: add configuration parameter for numThreads
    }

    /**
     * Top-level method for importing tables from CSV files into MacroBase SQL
     *
     * @return A DataFrame that contains the data loaded from the CSV file
     * @throws MacroBaseSQLException if there's an error parsing the CSV file
     */
    DataFrame importTableFromCsv(ImportCsv importStatement) throws MacroBaseSQLException {
        final String filename = importStatement.getFilename();
        final String tableName = importStatement.getTableName().toString();
        final Map<String, ColType> schema = importStatement.getSchema();
        try {
            DataFrame df = new CSVDataFrameParser(filename, schema).load();
            tablesInMemory.put(tableName, df);
            return df;
        } catch (Exception e) {
            throw new MacroBaseSQLException(e);
        }
    }

    /**
     * Top-level method for executing a SQL query in MacroBase SQL
     *
     * @return A DataFrame corresponding to the results of the query
     * @throws MacroBaseException If there's an error -- syntactic or logical -- processing the
     * query, an exception is thrown
     */
    DataFrame executeQuery(QueryBody query) throws MacroBaseException {
        if (query instanceof QuerySpecification) {
            QuerySpecification querySpec = (QuerySpecification) query;
            log.debug(querySpec.toString());
            return executeQuerySpec(querySpec);

        } else if (query instanceof DiffQuerySpecification) {
            DiffQuerySpecification diffQuery = (DiffQuerySpecification) query;
            log.debug(diffQuery.toString());
            return executeDiffQuerySpec(diffQuery);
        }
        throw new MacroBaseSQLException(
            "query of type " + query.getClass().getSimpleName() + " not yet supported");
    }

    /**
     * Execute a DIFF query, a query that's specific to MacroBase SQL (i.e., a query that may
     * contain DIFF and SPLIT operators).
     *
     * @return A DataFrame containing the results of the query
     * @throws MacroBaseException If there's an error -- syntactic or logical -- processing the
     * query, an exception is thrown
     */
    private DataFrame executeDiffQuerySpec(final DiffQuerySpecification diffQuery)
        throws MacroBaseException {
        final String outlierColName = "outlier_col";
        DataFrame dfToExplain;

        if (diffQuery.hasTwoArgs()) {
            // case 1: two separate subqueries
            final TableSubquery first = diffQuery.getFirst().get();
            final TableSubquery second = diffQuery.getSecond().get();

            // execute subqueries
            final DataFrame outliersDf = executeQuery(first.getQuery().getQueryBody());
            final DataFrame inliersDf = executeQuery(second.getQuery().getQueryBody());

            dfToExplain = concatOutliersAndInliers(outlierColName, outliersDf, inliersDf);
        } else {
            // case 2: single SPLIT (...) WHERE ... query
            final SplitQuery splitQuery = diffQuery.getSplitQuery().get();
            final Relation inputRelation = splitQuery.getInputRelation();

            if (inputRelation instanceof TableSubquery) {
                final QueryBody subquery = ((TableSubquery) inputRelation).getQuery()
                    .getQueryBody();
                dfToExplain = executeQuery(subquery);
            } else {
                // instance of Table
                dfToExplain = getTable(((Table) inputRelation).getName().toString());
            }

            // add outlier (binary) column by evaluating the WHERE clause
            final ModBitSet mask = getMask(dfToExplain, splitQuery.getWhereClause());
            final double[] outlierVals = new double[dfToExplain.getNumRows()];
            mask.stream().forEach((i) -> outlierVals[i] = 1.0);
            dfToExplain.addColumn(outlierColName, outlierVals);
        }

        List<String> explainCols = diffQuery.getAttributeCols().stream()
            .map(Identifier::getValue)
            .collect(toImmutableList());
        if ((explainCols.size() == 1) && explainCols.get(0).equals("*")) {
            // ON *, explore columns in DataFrame
            explainCols = findExplanationColumns(dfToExplain);
            log.info("Using " + Joiner.on(", ").join(explainCols)
                + " as candidate attributes for explanation");
        }

        // TODO: should be able to check this without having to execute the two subqueries
        if (!dfToExplain.getSchema().hasColumns(explainCols)) {
            throw new MacroBaseSQLException(
                "ON " + Joiner.on(", ").join(explainCols) + " not present in table");
        }

        // TODO: if an explainCol isn't in the SELECT clause, don't include it
        final double minRatioMetric = diffQuery.getMinRatioExpression().getMinRatio();
        final double minSupport = diffQuery.getMinSupportExpression().getMinSupport();
        final String ratioMetric = diffQuery.getRatioMetricExpr().getFuncName().toString();
        final int order = diffQuery.getMaxCombo().getValue();

        // execute diff
        final APLOutlierSummarizer summarizer = new APLOutlierSummarizer(true);
        summarizer.setRatioMetric(ratioMetric)
            .setMaxOrder(order)
            .setMinSupport(minSupport)
            .setMinRatioMetric(minRatioMetric)
            .setOutlierColumn(outlierColName)
            .setAttributes(explainCols)
            .setNumThreads(numThreads);

        try {
            summarizer.process(dfToExplain);
        } catch (Exception e) {
            // TODO: get rid of this Exception
            e.printStackTrace();
        }
        final DataFrame resultDf = summarizer.getResults().toDataFrame(explainCols);
        resultDf.renameColumn("outliers", "outlier_count");
        resultDf.renameColumn("count", "total_count");

        return evaluateSQLClauses(diffQuery, resultDf);
    }

    /**
     * Find columns that should be included in the "ON col1, col2, ..., coln" clause
     *
     * @return List of columns (as Strings)
     */
    private List<String> findExplanationColumns(DataFrame df) {
        Builder<String> builder = ImmutableList.builder();
        final boolean sample = df.getNumRows() > 1000;
        final int numRowsToSample = sample ? 1000 : df.getNumRows();
        final List<String> stringCols = df.getSchema().getColumnNamesByType(ColType.STRING);
        for (String colName : stringCols) {
            final String[] colValues = df.getStringColumnByName(colName);
            final Set<String> set = new HashSet<>();
            if (sample) {
                final Random random = new Random();
                for (int i = 0; i < 1000; ++i) {
                    set.add(colValues[random.nextInt(colValues.length)]);
                }
            } else {
                set.addAll(Arrays.asList(colValues));
            }
            if (set.size() < numRowsToSample / 4) {
                // if number of distinct elements is less than 1/4 the number of sampled rows,
                // include it
                builder.add(colName);
            }
        }
        return builder.build();
    }

    /**
     * Returns all values in the SELECT clause of a given query that are {@link FunctionCall}
     * objects, which are UDFs (e.g., "percentile(column_name)").
     *
     * @param select The Select clause
     * @return The items in the Select clause that correspond to UDFs returned as a List of {@link
     * SingleColumn}
     */
    private List<SingleColumn> getUDFsInSelect(final Select select) {
        final List<SingleColumn> udfs = new ArrayList<>();
        for (SelectItem item : select.getSelectItems()) {
            if (item instanceof SingleColumn) {
                final SingleColumn col = (SingleColumn) item;
                if (col.getExpression() instanceof FunctionCall) {
                    udfs.add(col);
                }
            }
        }
        return udfs;
    }

    /**
     * Concatenate two DataFrames -- outlier and inlier -- into a single DataFrame, with a new
     * column that stores 1 if the row is originally from the outlier DF and 0 if it's from the
     * inlier DF
     *
     * @param outlierColName The name of the binary column that denotes outlier/inlier
     * @param outliersDf outlier DataFrame
     * @param inliersDf inlier DataFrame
     * @return new DataFrame that contains rows from both DataFrames, along with the extra binary
     * column
     */
    private DataFrame concatOutliersAndInliers(final String outlierColName,
        final DataFrame outliersDf, final DataFrame inliersDf) {

        // Add column "outlier_col" to both outliers (all 1.0) and inliers (all 0.0)
        outliersDf.addColumn(outlierColName,
            DoubleStream.generate(() -> 1.0).limit(outliersDf.getNumRows()).toArray());
        inliersDf.addColumn(outlierColName,
            DoubleStream.generate(() -> 0.0).limit(inliersDf.getNumRows()).toArray());
        return DataFrame.unionAll(Lists.newArrayList(outliersDf, inliersDf));
    }

    /**
     * Evaluate standard SQL clauses: SELECT, WHERE, ORDER BY, and LIMIT. TODO: support GROUP BY and
     * HAVING clauses
     *
     * @param query the query that contains the clauses
     * @param df the DataFrame to apply these clauses to
     * @return a new DataFrame, the result of applying all of these clauses
     */
    private DataFrame evaluateSQLClauses(final QueryBody query, final DataFrame df)
        throws MacroBaseException {
        DataFrame resultDf = evaluateUDFs(df, getUDFsInSelect(query.getSelect()));
        resultDf = evaluateWhereClause(resultDf, query.getWhere());
        resultDf = evaluateSelectClause(resultDf, query.getSelect());
        // TODO: what if you order by something that's not in the SELECT clause?
        resultDf = evaluateOrderByClause(resultDf, query.getOrderBy());
        return evaluateLimitClause(resultDf, query.getLimit());
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
     * Execute a standard SQL query (i.e., a query that only contains ANSI SQL terms, and does not
     * contain any DIFF or SPLIT operators). For now, we ignore GROUP BY, HAVING, and JOIN clauses
     *
     * @return A DataFrame containing the results of the SQL query
     */
    private DataFrame executeQuerySpec(final QuerySpecification query)
        throws MacroBaseException {
        final Table table = (Table) query.getFrom().get();
        final DataFrame df = getTable(table.getName().toString());
        return evaluateSQLClauses(query, df);
    }

    /**
     * Get table as DataFrame that has previously been loaded into memory
     *
     * @param tableName String that uniquely identifies table
     * @return a shallow copy of the DataFrame for table; the original DataFrame is never returned,
     * so that we keep it immutable
     * @throws MacroBaseSQLException if the table has not been loaded into memory and does not
     * exist
     */
    private DataFrame getTable(String tableName) throws MacroBaseSQLException {
        if (!tablesInMemory.containsKey(tableName)) {
            throw new MacroBaseSQLException("Table " + tableName + " does not exist");
        }
        return tablesInMemory.get(tableName).copy();
    }

    /**
     * Evaluate only the UDFs of SQL query and return a new DataFrame with the UDF-generated columns
     * added to the input DataFrame. If there are no UDFs (i.e. @param udfCols is empty), the input
     * DataFrame is returned as is.
     *
     * @param inputDf The DataFrame to evaluate the UDFs on
     * @param udfCols The List of UDFs to evaluate
     */
    private DataFrame evaluateUDFs(final DataFrame inputDf, final List<SingleColumn> udfCols)
        throws MacroBaseException {

        // create shallow copy, so modifications don't persist on the original DataFrame
        final DataFrame resultDf = inputDf.copy();
        for (SingleColumn udfCol : udfCols) {
            final FunctionCall func = (FunctionCall) udfCol.getExpression();
            // for now, if UDF is a.b.c.d(), ignore "a.b.c."
            final String funcName = func.getName().getSuffix();
            // for now, assume func.getArguments returns at least 1 argument, always grab the first
            final MBFunction mbFunction = MBFunction.getFunction(funcName,
                func.getArguments().stream().map(Expression::toString).findFirst().get());

            // modify resultDf in place, add column; mbFunction is evaluated on input DataFrame
            resultDf.addColumn(udfCol.toString(), mbFunction.apply(inputDf));
        }
        return resultDf;
    }

    /**
     * Evaluate Select clause of SQL query, but only once all UDFs from the clause have been
     * removed. If the clause is 'SELECT *' the same DataFrame is returned unchanged. TODO: add
     * support for DISTINCT queries
     *
     * @param df The DataFrame to apply the Select clause on
     * @param select The Select clause
     * @return A new DataFrame with the result of the Select clause applied
     */
    private DataFrame evaluateSelectClause(DataFrame df, final Select select) {
        final List<SelectItem> items = select.getSelectItems();
        for (SelectItem item : items) {
            // If we find '*' -> relation is unchanged
            if (item instanceof AllColumns) {
                return df;
            }
        }
        final List<String> projections = items.stream().map(SelectItem::toString)
            .collect(toImmutableList());
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
        final Optional<Expression> whereClauseOpt) throws MacroBaseException {
        if (!whereClauseOpt.isPresent()) {
            return df;
        }
        final Expression whereClause = whereClauseOpt.get();
        final ModBitSet mask = getMask(df, whereClause);
        return df.filter(mask);
    }

    // ********************* Helper methods for evaluating Where clauses **********************

    /**
     * Recursive method that, given a Where clause, generates a boolean mask (a ModBitSet) applying the
     * clause to a DataFrame
     *
     * @throws MacroBaseSQLException Only comparison expressions (e.g., WHERE x = 42) and logical
     * AND/OR/NOT combinations of such expressions are supported; exception is thrown otherwise.
     */
    private ModBitSet getMask(DataFrame df, Expression whereClause) throws MacroBaseException {
        if (whereClause instanceof NotExpression) {
            final NotExpression notExpr = (NotExpression) whereClause;
            final ModBitSet mask = getMask(df, notExpr.getValue());
            mask.flip(0, df.getNumRows());
            return mask;

        } else if (whereClause instanceof LogicalBinaryExpression) {
            final LogicalBinaryExpression binaryExpr = (LogicalBinaryExpression) whereClause;
            final ModBitSet leftMask = getMask(df, binaryExpr.getLeft());
            final ModBitSet rightMask = getMask(df, binaryExpr.getRight());
            if (binaryExpr.getType() == Type.AND) {
                leftMask.and(rightMask);
                return leftMask;
            } else {
                // Type.OR
                leftMask.or(rightMask);
                return leftMask;
            }

        } else if (whereClause instanceof ComparisonExpression) {
            // base case
            final ComparisonExpression compareExpr = (ComparisonExpression) whereClause;
            final Expression left = compareExpr.getLeft();
            final Expression right = compareExpr.getRight();
            final ComparisonExpressionType type = compareExpr.getType();

            if (left instanceof Literal && right instanceof Literal) {
                final boolean val = left.equals(right);
                final ModBitSet mask = new ModBitSet(df.getNumRows());
                mask.set(0, df.getNumRows(), val);
                return mask;
            } else if (left instanceof Literal && right instanceof Identifier) {
                return maskForPredicate(df, (Literal) left, (Identifier) right, type);
            } else if (right instanceof Literal && left instanceof Identifier) {
                return maskForPredicate(df, (Literal) right, (Identifier) left, type);
            } else if (left instanceof FunctionCall && right instanceof Literal) {
                return maskForPredicate(df, (FunctionCall) left, (Literal) right, type);
            } else if (right instanceof FunctionCall && left instanceof Literal) {
                return maskForPredicate(df, (FunctionCall) right, (Literal) left, type);
            }
        }
        throw new MacroBaseSQLException("Boolean expression not supported");
    }

    private ModBitSet maskForPredicate(DataFrame df, FunctionCall func, Literal val,
                                       final ComparisonExpressionType type)
        throws MacroBaseException {
        final String funcName = func.getName().getSuffix();
        final MBFunction mbFunction = MBFunction.getFunction(funcName,
            func.getArguments().stream().map(Expression::toString).findFirst().get());
        final double[] col = mbFunction.apply(df);
        final DoublePredicate predicate = generateLambdaForPredicate(
            ((DoubleLiteral) val).getValue(), type);
        final ModBitSet mask = new ModBitSet(col.length);
        for (int i = 0; i < col.length; ++i) {
            if (predicate.test(col[i])) {
                mask.set(i);
            }
        }
        return mask;
    }


    /**
     * The base case for {@link QueryEngine#getMask(DataFrame, Expression)}; returns a boolean mask
     * (as a ModBitSet) for a single comparision expression (e.g., WHERE x = 42)
     *
     * @param df The DataFrame on which to evaluate the comparison expression
     * @param literal The constant argument in the expression (e.g., 42)
     * @param identifier The column variable argument in the expression (e.g., x)
     * @param compExprType One of =, !=, >, >=, <, <=, or IS DISTINCT FROM
     * @throws MacroBaseSQLException if the literal's type doesn't match the type of the column
     * variable, an exception is thrown
     */
    private ModBitSet maskForPredicate(final DataFrame df, final Literal literal,
                                       final Identifier identifier, final ComparisonExpressionType compExprType)
        throws MacroBaseSQLException {
        final String colName = identifier.getValue();
        final int colIndex;
        try {
            colIndex = df.getSchema().getColumnIndex(colName);
        } catch (UnsupportedOperationException e) {
            throw new MacroBaseSQLException(e.getMessage());
        }
        final ColType colType = df.getSchema().getColumnType(colIndex);

        if (colType == ColType.DOUBLE) {
            if (!(literal instanceof DoubleLiteral)) {
                throw new MacroBaseSQLException(
                    "Column " + colName + " has type " + colType + ", but " + literal
                        + " is not a DoubleLiteral");
            }

            return df.getMaskForFilter(colIndex,
                generateLambdaForPredicate(((DoubleLiteral) literal).getValue(), compExprType));
        } else {
            // colType == ColType.STRING
            if (literal instanceof StringLiteral) {
                return df.getMaskForFilter(colIndex,
                    generateLambdaForPredicate(((StringLiteral) literal).getValue(), compExprType));
            } else if (literal instanceof NullLiteral) {
                return df.getMaskForFilter(colIndex,
                    generateLambdaForPredicate(null, compExprType));
            } else {
                throw new MacroBaseSQLException(
                    "Column " + colName + " has type " + colType + ", but " + literal
                        + " is not StringLiteral");
            }
        }
    }

    /**
     * Return a Java Predicate expression for a given comparison type and constant value of type
     * double. (See {@link QueryEngine#generateLambdaForPredicate(String, ComparisonExpressionType)}
     * for handling a String argument.)
     *
     * @param y The constant value
     * @param compareExprType One of =, !=, >, >=, <, <=, or IS DISTINCT FROM
     * @return A {@link DoublePredicate}, that wraps the constant y in a closure
     * @throws MacroBaseSQLException If a comparsion type is passed in that is not supported, an
     * exception is thrown
     */
    private DoublePredicate generateLambdaForPredicate(double y,
        ComparisonExpressionType compareExprType) throws MacroBaseSQLException {
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
                throw new MacroBaseSQLException(compareExprType + " is not supported");
        }
    }

    /**
     * Return a Java Predicate expression for a given comparison type and constant value of type
     * String. (See {@link QueryEngine#generateLambdaForPredicate(double, ComparisonExpressionType)}
     * for handling a double argument.)
     *
     * @param y The constant value
     * @param compareExprType One of =, !=, >, >=, <, <=, or IS DISTINCT FROM
     * @return A {@link Predicate<Object>}, that wraps the constant y in a closure. A
     * Predicate<String> is not returned for compatibility with {@link DataFrame#filter(int,
     * Predicate)}.
     * @throws MacroBaseSQLException If a comparsion type is passed in that is not supported, an
     * exception is thrown
     */
    private Predicate<Object> generateLambdaForPredicate(final String y,
        final ComparisonExpressionType compareExprType) throws MacroBaseSQLException {
        switch (compareExprType) {
            case EQUAL:
                return (x) -> Objects.equals(x, y);
            case NOT_EQUAL:
            case IS_DISTINCT_FROM:
                // IS DISTINCT FROM is true when x and y have different values or
                // if one of them is NULL and the other isn't
                return (x) -> !Objects.equals(x, y);
            default:
                throw new MacroBaseSQLException(compareExprType + " is not supported");
        }
    }
}
