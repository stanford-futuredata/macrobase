package edu.stanford.futuredata.macrobase.sql;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import edu.stanford.futuredata.macrobase.analysis.MBFunction;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLOutlierSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.sql.tree.*;
import edu.stanford.futuredata.macrobase.sql.tree.LogicalBinaryExpression.Type;
import edu.stanford.futuredata.macrobase.sql.tree.SortItem.Ordering;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import edu.stanford.futuredata.macrobase.util.MacrobaseSQLException;

import java.util.*;
import java.util.function.DoublePredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueryEngineDistributed {

    private static final Logger log = LoggerFactory.getLogger(QueryEngineDistributed.class.getSimpleName());

    private final Map<String, DataFrame> tablesInMemory;
    private final int numThreads;
    private final SparkSession spark;

    QueryEngineDistributed(SparkSession spark) {
        this.spark = spark;
        tablesInMemory = new HashMap<>();
        numThreads = 1; // TODO: add configuration parameter for numThreads
    }

    /**
     * Top-level method for importing tables from CSV files into MacroBase SQL
     *
     * @return A DataFrame that contains the data loaded from the CSV file
     * @throws MacrobaseSQLException if there's an error parsing the CSV file
     */
    Dataset<Row> importTableFromCsv(ImportCsv importStatement) throws MacrobaseSQLException {
        final String fileName = importStatement.getFilename();
        final String tableName = importStatement.getTableName().toString();
        final Map<String, ColType> schema = importStatement.getSchema();
        try {
            // Distribute and parse
            JavaRDD<String[]> datasetRDD = spark.sparkContext().textFile(fileName, 1).toJavaRDD().mapPartitions(
                    (Iterator<String> iter) -> {
                        CsvParserSettings settings = new CsvParserSettings();
                        settings.getFormat().setLineSeparator("\n");
                        settings.setMaxCharsPerColumn(16384);
                        CsvParser csvParser = new CsvParser(settings);
                        List<String[]> parsedRows = new ArrayList<>();
                        while(iter.hasNext()) {
                            String row = iter.next();
                            String[] parsedRow = csvParser.parseLine(row);
                            parsedRows.add(parsedRow);
                        }
                        return parsedRows.iterator();
                    }, true
            );
            // Extract the header
            String[] header = datasetRDD.first();
            Map<Integer, ColType> indexToColTypeMap = new HashMap<>();
            for (int i = 0; i < header.length; i++) {
                if (schema.containsKey(header[i])) {
                    indexToColTypeMap.put(i, schema.get(header[i]));
                }
            }
            // Remove the header
            datasetRDD = datasetRDD.mapPartitionsWithIndex(
                    (Integer index, Iterator<String[]> iter) -> {
                        if (index == 0) {
                            iter.next();
                            iter.remove();
                        }
                        return iter;
                    }, true
            );
            List<StructField> fields = new ArrayList<>();
            for (int i = 0 ; i < header.length; i ++) {
                if (schema.containsKey(header[i])) {
                    String fieldName = header[i];
                    if (schema.get(fieldName) == ColType.STRING) {
                        StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
                        fields.add(field);
                    } else {
                        throw new MacrobaseSQLException("Only string supported in schema");
                    }
                }
            }
            JavaRDD<Row> datasetRowRDD = datasetRDD.map((String[] record) ->
            {
                List<String> rowList = new ArrayList<>();
                for (int i = 0; i < record.length; i++) {
                    if (indexToColTypeMap.containsKey(i))
                        rowList.add(record[i]);
                }
                return RowFactory.create(rowList.toArray());
            });
            Dataset<Row> df = spark.createDataFrame(datasetRowRDD, DataTypes.createStructType(fields));
            df.createOrReplaceTempView(tableName);
            return df;
        } catch (Exception e) {
            throw new MacrobaseSQLException(e.getMessage());
        }
    }

    /**
     * Top-level method for executing a SQL query in MacroBase SQL
     *
     * @return A DataFrame corresponding to the results of the query
     * @throws MacrobaseException If there's an error -- syntactic or logical -- processing the
     * query, an exception is thrown
     */
    Dataset<Row> executeQuery(Query query) throws MacrobaseException {
        QueryBody queryBody = query.getQueryBody();
        if (queryBody instanceof QuerySpecification) {
            QuerySpecification querySpec = (QuerySpecification) queryBody;
            String sqlString = SqlFormatter.formatSql(query, Optional.empty());
            log.info("Non-Diff Query: " + sqlString);
            log.debug(querySpec.toString());
            return spark.sql(sqlString);

        } else if (queryBody instanceof DiffQuerySpecification) {
            throw new MacrobaseSQLException("No diffs yet");
//            DiffQuerySpecification diffQuery = (DiffQuerySpecification) queryBody;
//            log.debug(diffQuery.toString());
//            return executeDiffQuerySpec(diffQuery);
        }
        throw new MacrobaseSQLException(
                "query of type " + queryBody.getClass().getSimpleName() + " not yet supported");
    }

    /**
     * Execute a DIFF query, a query that's specific to MacroBase SQL (i.e., a query that may
     * contain DIFF and SPLIT operators).
     *
     * @return A DataFrame containing the results of the query
     * @throws MacrobaseException If there's an error -- syntactic or logical -- processing the
     * query, an exception is thrown
     */
//    private DataFrame executeDiffQuerySpec(final DiffQuerySpecification diffQuery)
//            throws MacrobaseException {
//        final String outlierColName = "outlier_col";
//        DataFrame dfToExplain;
//
//        if (diffQuery.hasTwoArgs()) {
//            // case 1: two separate subqueries
//            final TableSubquery first = diffQuery.getFirst().get();
//            final TableSubquery second = diffQuery.getSecond().get();
//
//            // execute subqueries
//            final DataFrame outliersDf = executeQuery(first.getQuery());
//            final DataFrame inliersDf = executeQuery(second.getQuery());
//
//            dfToExplain = concatOutliersAndInliers(outlierColName, outliersDf, inliersDf);
//        } else {
//            // case 2: single SPLIT (...) WHERE ... query
//            final SplitQuery splitQuery = diffQuery.getSplitQuery().get();
//            final Relation inputRelation = splitQuery.getInputRelation();
//
//            if (inputRelation instanceof TableSubquery) {
//                final Query subquery = ((TableSubquery) inputRelation).getQuery();
//                dfToExplain = executeQuery(subquery);
//            } else {
//                // instance of Table
//                dfToExplain = getTable(((Table) inputRelation).getName().toString());
//            }
//
//            // add outlier (binary) column by evaluating the WHERE clause
//            final BitSet mask = getMask(dfToExplain, splitQuery.getWhereClause());
//            final double[] outlierVals = new double[dfToExplain.getNumRows()];
//            mask.stream().forEach((i) -> outlierVals[i] = 1.0);
//            dfToExplain.addColumn(outlierColName, outlierVals);
//        }
//
//        List<String> explainCols = diffQuery.getAttributeCols().stream()
//                .map(Identifier::getValue)
//                .collect(Collectors.toList());
//        if ((explainCols.size() == 1) && explainCols.get(0).equals("*")) {
//            // ON *, explore columns in DataFrame
//            explainCols = findExplanationColumns(dfToExplain);
//            log.info("Using " + Joiner.on(", ").join(explainCols)
//                    + " as candidate attributes for explanation");
//        }
//
//        // TODO: should be able to check this without having to execute the two subqueries
//        if (!dfToExplain.getSchema().hasColumns(explainCols)) {
//            throw new MacrobaseSQLException(
//                    "ON " + Joiner.on(", ").join(explainCols) + " not present in table");
//        }
//
//        // TODO: if an explainCol isn't in the SELECT clause, don't include it
//        final double minRatioMetric = diffQuery.getMinRatioExpression().getMinRatio();
//        final double minSupport = diffQuery.getMinSupportExpression().getMinSupport();
//        final String ratioMetric = diffQuery.getRatioMetricExpr().getFuncName().toString();
//        final int order = diffQuery.getMaxCombo().getValue();
//
//        // execute diff
//        final APLOutlierSummarizer summarizer = new APLOutlierSummarizer();
//        summarizer.setRatioMetric(ratioMetric)
//                .setMaxOrder(order)
//                .setMinSupport(minSupport)
//                .setMinRatioMetric(minRatioMetric)
//                .setOutlierColumn(outlierColName)
//                .setAttributes(explainCols)
//                .setNumThreads(numThreads);
//
//        try {
//            summarizer.process(dfToExplain);
//        } catch (Exception e) {
//            // TODO: get rid of this Exception
//            e.printStackTrace();
//        }
//        final DataFrame resultDf = summarizer.getResults().toDataFrame(explainCols);
//        resultDf.renameColumn("outliers", "outlier_count");
//        resultDf.renameColumn("count", "total_count");
//
//        return evaluateSQLClauses(diffQuery, resultDf);
//    }

    /**
     * Find columns that should be included in the "ON col1, col2, ..., coln" clause
     *
     * @return List of columns (as Strings)
     */
    private List<String> findExplanationColumns(DataFrame dfToExplain) {
        Builder<String> builder = ImmutableList.builder();
        final int numRowsToSample =
                dfToExplain.getNumRows() < 1000 ? dfToExplain.getNumRows() : 1000;
        final List<String> stringCols = dfToExplain.getSchema()
                .getColumnNamesByType(ColType.STRING);
        for (String colName : stringCols) {
            final String[] colValues = dfToExplain.getStringColumnByName(colName);
            final Set<String> set = new HashSet<>();
            set.addAll(Arrays.asList(colValues).subList(0, numRowsToSample));
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
                DoubleStream.generate(() -> 0.0).limit(outliersDf.getNumRows()).toArray());
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
            throws MacrobaseException {
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
            throws MacrobaseException {
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
     * @throws MacrobaseSQLException if the table has not been loaded into memory and does not
     * exist
     */
    private DataFrame getTable(String tableName) throws MacrobaseSQLException {
        if (!tablesInMemory.containsKey(tableName)) {
            throw new MacrobaseSQLException("Table " + tableName + " does not exist");
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
            throws MacrobaseException {

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
                .collect(Collectors.toList());
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
                                          final Optional<Expression> whereClauseOpt) throws MacrobaseException {
        if (!whereClauseOpt.isPresent()) {
            return df;
        }
        final Expression whereClause = whereClauseOpt.get();
        final BitSet mask = getMask(df, whereClause);
        return df.filter(mask);
    }

    // ********************* Helper methods for evaluating Where clauses **********************

    /**
     * Recursive method that, given a Where clause, generates a boolean mask (a BitSet) applying the
     * clause to a DataFrame
     *
     * @throws MacrobaseSQLException Only comparison expressions (e.g., WHERE x = 42) and logical
     * AND/OR/NOT combinations of such expressions are supported; exception is thrown otherwise.
     */
    private BitSet getMask(DataFrame df, Expression whereClause) throws MacrobaseException {
        if (whereClause instanceof NotExpression) {
            final NotExpression notExpr = (NotExpression) whereClause;
            final BitSet mask = getMask(df, notExpr.getValue());
            mask.flip(0, df.getNumRows());
            return mask;

        } else if (whereClause instanceof LogicalBinaryExpression) {
            final LogicalBinaryExpression binaryExpr = (LogicalBinaryExpression) whereClause;
            final BitSet leftMask = getMask(df, binaryExpr.getLeft());
            final BitSet rightMask = getMask(df, binaryExpr.getRight());
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
                final BitSet mask = new BitSet(df.getNumRows());
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
        throw new MacrobaseSQLException("Boolean expression not supported");
    }

    private BitSet maskForPredicate(DataFrame df, FunctionCall func, Literal val,
                                    final ComparisonExpressionType type)
            throws MacrobaseException {
        final String funcName = func.getName().getSuffix();
        final MBFunction mbFunction = MBFunction.getFunction(funcName,
                func.getArguments().stream().map(Expression::toString).findFirst().get());
        final double[] col = mbFunction.apply(df);
        final DoublePredicate predicate = generateLambdaForPredicate(
                ((DoubleLiteral) val).getValue(), type);
        final BitSet mask = new BitSet(col.length);
        for (int i = 0; i < col.length; ++i) {
            if (predicate.test(col[i])) {
                mask.set(i);
            }
        }
        return mask;
    }


    /**
     * The base case for {@link QueryEngineDistributed#getMask(DataFrame, Expression)}; returns a boolean mask
     * (as a BitSet) for a single comparision expression (e.g., WHERE x = 42)
     *
     * @param df The DataFrame on which to evaluate the comparison expression
     * @param literal The constant argument in the expression (e.g., 42)
     * @param identifier The column variable argument in the expression (e.g., x)
     * @param compExprType One of =, !=, >, >=, <, <=, or IS DISTINCT FROM
     * @throws MacrobaseSQLException if the literal's type doesn't match the type of the column
     * variable, an exception is thrown
     */
    private BitSet maskForPredicate(final DataFrame df, final Literal literal,
                                    final Identifier identifier, final ComparisonExpressionType compExprType)
            throws MacrobaseSQLException {
        final String colName = identifier.getValue();
        final int colIndex;
        try {
            colIndex = df.getSchema().getColumnIndex(colName);
        } catch (UnsupportedOperationException e) {
            throw new MacrobaseSQLException(e.getMessage());
        }
        final ColType colType = df.getSchema().getColumnType(colIndex);

        if (colType == ColType.DOUBLE) {
            if (!(literal instanceof DoubleLiteral)) {
                throw new MacrobaseSQLException(
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
                throw new MacrobaseSQLException(
                        "Column " + colName + " has type " + colType + ", but " + literal
                                + " is not StringLiteral");
            }
        }
    }

    /**
     * Return a Java Predicate expression for a given comparison type and constant value of type
     * double. (See {@link QueryEngineDistributed#generateLambdaForPredicate(String, ComparisonExpressionType)}
     * for handling a String argument.)
     *
     * @param y The constant value
     * @param compareExprType One of =, !=, >, >=, <, <=, or IS DISTINCT FROM
     * @return A {@link DoublePredicate}, that wraps the constant y in a closure
     * @throws MacrobaseSQLException If a comparsion type is passed in that is not supported, an
     * exception is thrown
     */
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

    /**
     * Return a Java Predicate expression for a given comparison type and constant value of type
     * String. (See {@link QueryEngineDistributed#generateLambdaForPredicate(double, ComparisonExpressionType)}
     * for handling a double argument.)
     *
     * @param y The constant value
     * @param compareExprType One of =, !=, >, >=, <, <=, or IS DISTINCT FROM
     * @return A {@link Predicate<Object>}, that wraps the constant y in a closure. A
     * Predicate<String> is not returned for compatibility with {@link DataFrame#filter(int,
     * Predicate)}.
     * @throws MacrobaseSQLException If a comparsion type is passed in that is not supported, an
     * exception is thrown
     */
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
