package edu.stanford.futuredata.macrobase.sql;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static edu.stanford.futuredata.macrobase.sql.tree.ComparisonExpressionType.EQUAL;
import static java.util.stream.DoubleStream.concat;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import edu.stanford.futuredata.macrobase.analysis.MBFunction;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLOutlierSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.util.ModBitSet;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.sql.tree.AliasedRelation;
import edu.stanford.futuredata.macrobase.sql.tree.AllColumns;
import edu.stanford.futuredata.macrobase.sql.tree.ComparisonExpression;
import edu.stanford.futuredata.macrobase.sql.tree.ComparisonExpressionType;
import edu.stanford.futuredata.macrobase.sql.tree.DereferenceExpression;
import edu.stanford.futuredata.macrobase.sql.tree.DiffQuerySpecification;
import edu.stanford.futuredata.macrobase.sql.tree.DoubleLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.Expression;
import edu.stanford.futuredata.macrobase.sql.tree.FunctionCall;
import edu.stanford.futuredata.macrobase.sql.tree.Identifier;
import edu.stanford.futuredata.macrobase.sql.tree.ImportCsv;
import edu.stanford.futuredata.macrobase.sql.tree.Join;
import edu.stanford.futuredata.macrobase.sql.tree.JoinCriteria;
import edu.stanford.futuredata.macrobase.sql.tree.JoinOn;
import edu.stanford.futuredata.macrobase.sql.tree.JoinUsing;
import edu.stanford.futuredata.macrobase.sql.tree.Literal;
import edu.stanford.futuredata.macrobase.sql.tree.LogicalBinaryExpression;
import edu.stanford.futuredata.macrobase.sql.tree.LogicalBinaryExpression.Type;
import edu.stanford.futuredata.macrobase.sql.tree.NaturalJoin;
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
import java.util.ArrayList;
import java.util.Arrays;
import edu.stanford.futuredata.macrobase.analysis.summary.util.ModBitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.DoublePredicate;
import java.util.function.Predicate;
import java.util.stream.DoubleStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueryEngine {

    private static final Logger log = LoggerFactory.getLogger(QueryEngine.class.getSimpleName());

    private final Map<String, DataFrame> tablesInMemory;
    private final int numThreads;
    private final boolean useHashJoin;

    QueryEngine() {
        tablesInMemory = new HashMap<>();
        numThreads = 1; // TODO: add configuration parameter for numThreads
        useHashJoin = true;
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
        final double minRatioMetric = diffQuery.getMinRatioExpression().getMinRatio();
        final double minSupport = diffQuery.getMinSupportExpression().getMinSupport();
        final String ratioMetric = diffQuery.getRatioMetricExpr().getFuncName().toString();
        final int order = diffQuery.getMaxCombo().getValue();
        List<String> explainCols = diffQuery.getAttributeCols().stream()
            .map(Identifier::getValue)
            .collect(toImmutableList());

        DataFrame dfToExplain;
        double[][] aggregateColumns = null;

        if (diffQuery.hasTwoArgs()) {
            // case 1: two separate subqueries
            final TableSubquery first = diffQuery.getFirst().get();
            final TableSubquery second = diffQuery.getSecond().get();

            // DIFF-JOIN optimization
            if (matchesDiffJoinCriteria(first.getQuery().getQueryBody(),
                second.getQuery().getQueryBody())) {
                final Join firstJoin = (Join) ((QuerySpecification) first.getQuery().getQueryBody())
                    .getFrom().get();
                final Join secondJoin = (Join) ((QuerySpecification) second.getQuery()
                    .getQueryBody()).getFrom().get();

                final DataFrame outlierDf = getDataFrameForRelation(firstJoin.getLeft()); // table R
                final DataFrame inlierDf = getDataFrameForRelation(secondJoin.getLeft()); // table S
                final DataFrame common = getDataFrameForRelation(firstJoin.getRight()); // table T

                final Optional<JoinCriteria> joinCriteriaOpt = firstJoin.getCriteria();
                if (!joinCriteriaOpt.isPresent()) {
                    throw new MacroBaseSQLException(
                        "No clause (e.g., ON, USING) specified in JOIN");
                }

                final String joinColumn = getJoinColumn(joinCriteriaOpt.get(),
                    outlierDf.getSchema(),
                    common.getSchema()); // column A1

                dfToExplain = evaluateDiffJoin(outlierDf, inlierDf, common, joinColumn, explainCols,
                    minRatioMetric);
                final double[] countCol = concat(
                    DoubleStream.generate(() -> 1.0).limit(outlierDf.getNumRows()),
                    DoubleStream.generate(() -> 1.0).limit(inlierDf.getNumRows())
                ).toArray();
                final double[] outlierCol = concat(
                    DoubleStream.generate(() -> 1.0).limit(outlierDf.getNumRows()),
                    DoubleStream.generate(() -> 0.0).limit(inlierDf.getNumRows())
                ).toArray();
                aggregateColumns = new double[2][];
                aggregateColumns[0] = outlierCol;
                aggregateColumns[1] = countCol;
            } else {
                // execute subqueries
                final DataFrame outliersDf = executeQuery(first.getQuery().getQueryBody());
                final DataFrame inliersDf = executeQuery(second.getQuery().getQueryBody());

                dfToExplain = concatOutliersAndInliers(outlierColName, outliersDf, inliersDf);
            }
        } else {
            // case 2: single SPLIT (...) WHERE ... query
            final SplitQuery splitQuery = diffQuery.getSplitQuery().get();
            final Relation relationToExplain = splitQuery.getInputRelation();
            dfToExplain = getDataFrameForRelation(relationToExplain);

            // add outlier (binary) column by evaluating the WHERE clause
            final ModBitSet mask = getMask(dfToExplain, splitQuery.getWhereClause());
            final double[] outlierVals = new double[dfToExplain.getNumRows()];
            mask.stream().forEach((i) -> outlierVals[i] = 1.0);
            dfToExplain.addColumn(outlierColName, outlierVals);
        }

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
        // execute diff
        final APLOutlierSummarizer summarizer = new APLOutlierSummarizer(true);
        summarizer.setRatioMetric(ratioMetric)
            .setMaxOrder(order)
            .setMinSupport(minSupport)
            .setMinRatioMetric(minRatioMetric)
            .setOutlierColumn(outlierColName)
            .setAttributes(explainCols)
            .setNumThreads(numThreads)
            .setAnti(diffQuery.isAnti());

        if (aggregateColumns != null) {
            summarizer.setGlobalAggregateCols(aggregateColumns);
        }

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
     * Execute DIFF-JOIN query using co-optimized algorithm. NOTE: Must be a Primary Key-Foreign Key
     * Join. TODO: We make the following assumptions in the method below:
     * 1) The two joins are both inner joins over the same, single column, which is of type String
     * 2) The ratio metric is global_ratio
     *
     *  R     S          T
     * ---   ---   -------------
     *  a     a     a | CA | v1
     *  a     b     b | CA | v2
     *  b     c     c | TX | v1
     *  b     d     d | TX | v2
     *  e           e | FL | v1
     *
     * @return result of the DIFF JOIN
     */
    private DataFrame evaluateDiffJoin(final DataFrame outlierDf, final DataFrame inlierDf,
        final DataFrame common, final String joinColumn, final List<String> explainColumnNames,
        final double minRatioMetric) {

        final List<String> explainColsInCommon = Lists.newArrayList(common.getSchema().getColumnNames());
        explainColsInCommon.retainAll(explainColumnNames);

        final int numOutliers = outlierDf.getNumRows();
        final int numInliers = inlierDf.getNumRows();
        log.info("Num Outliers:  {}, num inliers: {}", numOutliers, numInliers);
        // NOTE: we assume that, because it's a PK-FK join, every value in the foreign key column
        // will appear in the primary key (but not vice versa). This means that evaluating the join
        // will never remove tuples from or add tuples to the inlier and outlier DataFrames. This allows
        // us to calculate the minRatioThreshold based on the total number of outliers and inliers.
        final double globalRatioDenom =
            numOutliers / (numOutliers + numInliers + 0.0);
        final double minRatioThreshold = minRatioMetric * globalRatioDenom;

        final String[] outlierProjected = outlierDf.project(joinColumn).getStringColumn(0);
        final String[] inlierProjected = inlierDf.project(joinColumn).getStringColumn(0);

        // 1) Execute \delta(\proj_{A1} R, \proj_{A1} S);
        final long foreignKeyDiff = System.currentTimeMillis();
        final Set<String> candidateForeignKeys = foreignKeyDiff(outlierProjected, inlierProjected,
            minRatioThreshold); // returns K, the candidate keys that exceeded the minRatioThreshold.
        // K may contain false positives, though (support threshold hasn't been applied yet)
        log.info("Foreign key diff time: {} ms", System.currentTimeMillis() - foreignKeyDiff);
        log.info("Num candidate foreign keys: {}", candidateForeignKeys.size());

        if (candidateForeignKeys.isEmpty()) {
            return new DataFrame();
        }

        // Keep track of candidates in each column, needed for order-2 and order-3 combinations
        final long semiJoinAndMergeTime = System.currentTimeMillis();
        // 2) Execute K \semijoin T, to get V, the values in T associated with the candidate keys,
        //    and merge common values that distinct keys may map to
        final Map<String, Integer> colValuesToIndices = semiJoinAndMerge(
            candidateForeignKeys, // K
            common.getStringColumnByName(joinColumn),
            common.getStringColsByName(explainColsInCommon)); // T
        log.info("Semi-join and merge time: {} ms",
            System.currentTimeMillis() - semiJoinAndMergeTime);

        final DataFrame toReturn = diffJoinAndConcat(outlierDf, inlierDf, common, joinColumn,
            colValuesToIndices);
        return toReturn;
    }

    @SuppressWarnings("Duplicates")
    private DataFrame diffJoinAndConcat(DataFrame outlierDf, DataFrame inlierDf, DataFrame common,
        final String joinColumn, Map<String, Integer> colValuesToIndices) {
        log.info("Num candidate values: {}", colValuesToIndices.size());

        final DataFrame outliersDf = diffJoinSingle(outlierDf, common, joinColumn,
            colValuesToIndices);
        final DataFrame inliersDf = diffJoinSingle(inlierDf, common, joinColumn,
            colValuesToIndices);
        return concatOutliersAndInliers("outlier_col",
            outliersDf,
            inliersDf
        );
    }

    private DataFrame diffJoinSingle(DataFrame bigger, DataFrame smaller, String joinColumn,
        Map<String, Integer> colValuesToIndices) {
        final long startTime = System.currentTimeMillis();
        final Map<String, List<String>> biggerStringResults = new HashMap<>();
        final Map<String, List<String>> smallerStringResults = new HashMap<>();
        for (String colName : bigger.getSchema().getColumnNamesByType(ColType.STRING)) {
            biggerStringResults.put(colName, new LinkedList<>());
        }
        for (String colName : smaller.getSchema().getColumnNamesByType(ColType.STRING)) {
            if (colName.equals(joinColumn)) {
                continue;
            }
            smallerStringResults.put(colName, new LinkedList<>());
        }

        // double column values that will be added to the DataFrame
        final Map<String, List<Double>> biggerDoubleResults = new HashMap<>();
        final Map<String, List<Double>> smallerDoubleResults = new HashMap<>();
        for (String colName : bigger.getSchema().getColumnNamesByType(ColType.DOUBLE)) {
            biggerDoubleResults.put(colName, new LinkedList<>());
        }
        for (String colName : smaller.getSchema().getColumnNamesByType(ColType.DOUBLE)) {
            if (colName.equals(joinColumn)) {
                continue;
            }
            smallerDoubleResults.put(colName, new LinkedList<>());
        }
        hashJoinWithIndex(bigger, smaller, joinColumn, colValuesToIndices, biggerStringResults,
            smallerStringResults, biggerDoubleResults, smallerDoubleResults);
        log.info("Diff Join Single: {} ms", System.currentTimeMillis() - startTime);
        return joinResultToDataFrame("small", "big", bigger.getSchema(),
            smaller.getSchema(), joinColumn, biggerStringResults, smallerStringResults,
            biggerDoubleResults, smallerDoubleResults);
    }

    private Set<String> foreignKeyDiff(final String[] outliers, final String[] inliers,
        double minRatioThreshold) {
        final Map<String, Integer> outlierCounts = new HashMap<>();
        log.info("Starting outliers");
        //noinspection Duplicates
        for (final String outlier : outliers) {
            outlierCounts.merge(outlier, 1, (a, b) -> a + b);
        }
        log.info("Starting inliers");
        final Map<String, Integer> inlierCounts = new HashMap<>();
        //noinspection Duplicates
        for (final String inlier : inliers) {
            inlierCounts.merge(inlier, 1, (a, b) -> a + b);
        }
        // Generate candidates based on min ratio
        final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (Entry<String, Integer> entry : outlierCounts
            .entrySet()) {
            final int numOutliers = entry.getValue();
            final int numInliers = inlierCounts.getOrDefault(entry.getKey(), 0);
            if ((numOutliers / (numOutliers + numInliers + 0.0))
                >= minRatioThreshold) {
                builder.add(entry.getKey());
            }
        }
        return builder.build();
    }

    private Map<String, Integer> semiJoinAndMerge(final Set<String> candidateForeignKeys,
        final String[] primaryKeyColumn, List<String[]> attrValues) {

        int numAdditionalValues = 0;
        final int numRows = attrValues.get(0).length;

        if (candidateForeignKeys.isEmpty()) {
            log.info("candidateForeignKeys is empty");
        }

        final int numCols = attrValues.size();
        final Set<String>[] attrCandidatesByColumn = new Set[numCols];
        for (int i = 0; i < numCols; ++i) {
            attrCandidatesByColumn[i] = new HashSet<>();
        }
        final Map<String, Integer> colValuesToIndices = new HashMap<>();

        // 1) K \semijoin T: Go through the primary key column and see what candidateForeignKeys are contained.
        //    For every match, save the corresponding values
        for (int i = 0; i < numRows; ++i) {
            final String primaryKey = primaryKeyColumn[i];
            if (candidateForeignKeys.contains(primaryKey)) {
                colValuesToIndices.put(primaryKey, i);

                // extract the corresponding values for the candidate key
                for (int j = 0; j < numCols; ++j) {
                    final String[] attrValuesCol = attrValues.get(j);
                    final String val = attrValuesCol[i];
                    attrCandidatesByColumn[j].add(val);
                }
            }
        }

        // 2) Go through again and check which saved values from the first pass map to new
        //    primary keys. If we find any new ones, add them to colValuesToIndices, so we
        //    can do the join
        for (int j = 0; j < numCols; ++j) {
            final String[] attrValuesCol = attrValues.get(j);
            for (int i = 0; i < numRows; ++i) {
                final String val = attrValuesCol[i];
                if (!attrCandidatesByColumn[j].contains(val)) {
                    // never found in the first pass
                    continue;
                }
                // extract the corresponding foreign key, merge the foreign key bitmaps
                final String primaryKey = primaryKeyColumn[i];
                if (!candidateForeignKeys.contains(primaryKey)) {
                    // if not found in the first pass, add it
                    colValuesToIndices.put(primaryKey, i);
                    ++numAdditionalValues;
                }
            }
        }
        log.info("Num additional values: {}", numAdditionalValues);
        return colValuesToIndices;
    }

    /**
     * @return true both relations are NATURAL JOIN subqueries that share a common table, e.g., R
     * \join T, and S \join T TODO: enforce FK-PK Join
     */
    private boolean matchesDiffJoinCriteria(final QueryBody first, final QueryBody second) {
        if (!(first instanceof QuerySpecification) || !(second instanceof QuerySpecification)) {
            return false;
        }
        final QuerySpecification firstQuerySpec = (QuerySpecification) first;
        final QuerySpecification secondQuerySpec = (QuerySpecification) second;
        final Relation firstRelation = firstQuerySpec.getFrom().get();
        final Relation secondRelation = secondQuerySpec.getFrom().get();

        if (!(firstRelation instanceof Join) || !(secondRelation instanceof Join)) {
            return false;
        }

        final Join firstJoin = (Join) firstRelation;
        final Join secondJoin = (Join) secondRelation;

        if (!(firstJoin.getCriteria().get() instanceof NaturalJoin) || !(secondJoin.getCriteria()
            .get() instanceof NaturalJoin)) {
            return false;
        }
        // TODO: my not necessarily be R \join T and S \join T; could be T \join R and T \join S. Need to support both
        return (firstJoin.getRight().equals(secondJoin.getRight()) &&
            !firstJoin.getLeft().equals(secondJoin.getLeft()));
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
    private DataFrame evaluateOrderByClause(DataFrame df, Optional<OrderBy> orderByOpt)
        throws MacroBaseSQLException {
        if (!orderByOpt.isPresent()) {
            return df;
        }
        final OrderBy orderBy = orderByOpt.get();
        // For now, we only support sorting by a single column
        // TODO: support multi-column sort
        final SortItem sortItem = orderBy.getSortItems().get(0);
        final Expression sortKey = sortItem.getSortKey();
        final String sortCol;
        if (sortKey instanceof Identifier) {
            sortCol = ((Identifier) sortKey).getValue();
        } else if (sortKey instanceof DereferenceExpression) {
            sortCol = sortKey.toString();
        } else {
            throw new MacroBaseSQLException("Unsupported expression type in ORDER BY");
        }
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
        final Relation from = query.getFrom().get();
        final DataFrame df;
        if (from instanceof Join) {
            final Join join = (Join) from;
            df = evaluateJoin(join);
        } else if (from instanceof TableSubquery) {
            final TableSubquery subquery = (TableSubquery) from;
            df = executeQuery(subquery.getQuery().getQueryBody());
        } else if (from instanceof Table) {
            final Table table = (Table) from;
            df = getTable(table.getName().toString());
        } else {
            throw new MacroBaseSQLException("Unsupported argument in FROM clause");
        }
        return evaluateSQLClauses(query, df);
    }

    /**
     * TODO
     */
    private DataFrame evaluateJoin(Join join) throws MacroBaseException {
        final long startTime = System.currentTimeMillis();
        final DataFrame left = getDataFrameForRelation(join.getLeft());
        final DataFrame right = getDataFrameForRelation(join.getRight());

        final boolean leftSmaller = left.getNumRows() < right.getNumRows();
        final DataFrame smaller = leftSmaller ? left : right;
        final DataFrame bigger = leftSmaller ? right : left;

        final String smallerName = leftSmaller ? getName(join.getLeft()) : getName(join.getRight());
        final String biggerName = leftSmaller ? getName(join.getRight()) : getName(join.getLeft());

        final Optional<JoinCriteria> joinCriteriaOpt = join.getCriteria();
        if (!joinCriteriaOpt.isPresent()) {
            throw new MacroBaseSQLException("No clause (e.g., ON, USING) specified in JOIN");
        }

        // Right now, we only support equality joins on a single column; this is enforced in
        // getJoinColumn
        final Schema biggerSchema = bigger.getSchema();
        final Schema smallerSchema = smaller.getSchema();
        final String joinColumn = getJoinColumn(joinCriteriaOpt.get(), biggerSchema, smallerSchema);
        switch (join.getType()) {
            case INNER:
                final int biggerColIndex, smallerColIndex;
                try {
                    biggerColIndex = biggerSchema.getColumnIndex(joinColumn);
                    smallerColIndex = smallerSchema.getColumnIndex(joinColumn);
                } catch (UnsupportedOperationException e) {
                    throw new MacroBaseSQLException(e.getMessage());
                }
                final ColType biggerColType = bigger.getSchema().getColumnType(biggerColIndex);
                final ColType smallerColType = smaller.getSchema().getColumnType(smallerColIndex);
                if (biggerColType != smallerColType) {
                    throw new MacroBaseSQLException(
                        "Column " + joinColumn + " has type " + joinColumn + " in one table but "
                            + " type " + joinColumn + " in the other");
                }

                // String column values that will be added to DataFrame
                final Map<String, List<String>> biggerStringResults = new HashMap<>();
                final Map<String, List<String>> smallerStringResults = new HashMap<>();
                for (String colName : biggerSchema.getColumnNamesByType(ColType.STRING)) {
                    biggerStringResults.put(colName, new LinkedList<>());
                }
                for (String colName : smallerSchema.getColumnNamesByType(ColType.STRING)) {
                    if (colName.equals(joinColumn)) {
                        continue;
                    }
                    smallerStringResults.put(colName, new LinkedList<>());
                }

                // double column values that will be added to the DataFrame
                final Map<String, List<Double>> biggerDoubleResults = new HashMap<>();
                final Map<String, List<Double>> smallerDoubleResults = new HashMap<>();
                for (String colName : biggerSchema.getColumnNamesByType(ColType.DOUBLE)) {
                    biggerDoubleResults.put(colName, new LinkedList<>());
                }
                for (String colName : smallerSchema.getColumnNamesByType(ColType.DOUBLE)) {
                    if (colName.equals(joinColumn)) {
                        continue;
                    }
                    smallerDoubleResults.put(colName, new LinkedList<>());
                }

                if (useHashJoin) {
                    log.info("Using hash join");
                    hashJoin(bigger, smaller, joinColumn, biggerStringResults, smallerStringResults,
                        biggerDoubleResults, smallerDoubleResults);
                } else {
                    log.info("Using nested loops join");
                    nestedLoopsJoin(bigger, smaller, getJoinLambda(biggerColIndex, smallerColIndex,
                        biggerColType), biggerStringResults, smallerStringResults,
                        biggerDoubleResults, smallerDoubleResults);
                }
                log.info("Time spent in Join: {} ms", System.currentTimeMillis() - startTime);

                return joinResultToDataFrame(smallerName, biggerName, biggerSchema, smallerSchema,
                    joinColumn, biggerStringResults, smallerStringResults, biggerDoubleResults,
                    smallerDoubleResults);
            default:
                throw new MacroBaseSQLException("Join type " + join.getType() + "not supported");
        }
    }

    private DataFrame joinResultToDataFrame(String smallerName, String biggerName,
        Schema biggerSchema, Schema smallerSchema, String joinColumn,
        Map<String, List<String>> biggerStringResults,
        Map<String, List<String>> smallerStringResults,
        Map<String, List<Double>> biggerDoubleResults,
        Map<String, List<Double>> smallerDoubleResults) {
        final DataFrame df = new DataFrame();
        // Add String results
        for (String colName : biggerStringResults.keySet()) {
            final String colNameForOutput =
                smallerSchema.hasColumn(colName) && !colName.equals(joinColumn) ? biggerName
                    + "." + colName : colName;
            df.addColumn(colNameForOutput,
                biggerStringResults.get(colName).toArray(new String[0]));
        }
        for (String colName : smallerStringResults.keySet()) {
            final String colNameForOutput =
                biggerSchema.hasColumn(colName) && !colName.equals(joinColumn) ? smallerName
                    + "." + colName : colName;
            df.addColumn(colNameForOutput,
                smallerStringResults.get(colName).toArray(new String[0]));
        }
        // Add double results
        for (String colName : biggerDoubleResults.keySet()) {
            final String colNameForOutput =
                smallerSchema.hasColumn(colName) && !colName.equals(joinColumn) ? biggerName
                    + "." + colName : colName;
            df.addColumn(colNameForOutput,
                biggerDoubleResults.get(colName).stream().mapToDouble((x) -> x).toArray());
        }
        for (String colName : smallerDoubleResults.keySet()) {
            final String colNameForOutput =
                biggerSchema.hasColumn(colName) && !colName.equals(joinColumn) ? smallerName
                    + "." + colName : colName;
            df.addColumn(colNameForOutput,
                smallerDoubleResults.get(colName).stream().mapToDouble((x) -> x).toArray());
        }
        return df;
    }

    /**
     * Evaluate join using hash-join algorithm
     */
    private void hashJoinWithIndex(final DataFrame bigger, final DataFrame smaller,
        final String joinColumn,
        final Map<String, Integer> colValuesToIndices,
        final Map<String, List<String>> biggerStringResults,
        final Map<String, List<String>> smallerStringResults,
        final Map<String, List<Double>> biggerDoubleResults,
        final Map<String, List<Double>> smallerDoubleResults) {

        final String[] biggerColumn = bigger.project(joinColumn).getStringColumn(0);

        for (int i = 0; i < biggerColumn.length; ++i) {
            final String value = biggerColumn[i];
            final Integer index = colValuesToIndices.get(value);
            if (index != null) {
                final Row biggerRow = bigger.getRow(i);
                addResultToJoinOutput(biggerStringResults, smallerStringResults,
                    biggerDoubleResults, smallerDoubleResults, biggerRow, smaller.getRow(index));
            }
        }
    }

    /**
     * Evaluate join using hash-join algorithm
     */
    private void hashJoin(final DataFrame bigger, final DataFrame smaller,
        final String joinColumn,
        final Map<String, List<String>> biggerStringResults,
        final Map<String, List<String>> smallerStringResults,
        final Map<String, List<Double>> biggerDoubleResults,
        final Map<String, List<Double>> smallerDoubleResults) {

        final String[] smallerColumn = smaller.project(joinColumn).getStringColumn(0);
        Map<String, List<Integer>> colValuesToIndices = new HashMap<>();
        for (int i = 0; i < smallerColumn.length; ++i) {
            final String value = smallerColumn[i];
            List<Integer> list = colValuesToIndices.computeIfAbsent(value, k -> new ArrayList<>());
            list.add(i);
        }

        final String[] biggerColumn = bigger.project(joinColumn).getStringColumn(0);

        for (int i = 0; i < biggerColumn.length; ++i) {
            final String value = biggerColumn[i];
            List<Integer> indices = colValuesToIndices.get(value);
            if (indices != null) {
                final Row biggerRow = bigger.getRow(i);
                for (int j : indices) {
                    addResultToJoinOutput(biggerStringResults, smallerStringResults,
                        biggerDoubleResults, smallerDoubleResults, biggerRow, smaller.getRow(j));
                }
            }
        }
    }

    /**
     * Evaluate join using nested loops algorithm
     */
    private void nestedLoopsJoin(final DataFrame bigger, final DataFrame smaller,
        final BiPredicate<Row, Row> lambda,
        final Map<String, List<String>> biggerStringResults,
        final Map<String, List<String>> smallerStringResults,
        final Map<String, List<Double>> biggerDoubleResults,
        final Map<String, List<Double>> smallerDoubleResults) throws MacroBaseSQLException {
        for (Row bigRow : bigger.getRowIterator()) {
            for (Row smallRow : smaller.getRowIterator()) {
                if (lambda.test(bigRow, smallRow)) {
                    addResultToJoinOutput(biggerStringResults, smallerStringResults,
                        biggerDoubleResults, smallerDoubleResults, bigRow, smallRow);

                }
            }
        }
    }

    /**
     * TODO
     */
    private String getName(Relation relation) throws MacroBaseSQLException {
        if (relation instanceof Table) {
            return ((Table) relation).getName().toString();
        } else if (relation instanceof AliasedRelation) {
            return ((AliasedRelation) relation).getAlias().getValue();
        } else {
            throw new MacroBaseSQLException("Not a supported relation for getName");
        }
    }

    // ********************* Helper methods for evaluating Join expressions **********************

    /**
     * TODO
     */
    private void addResultToJoinOutput(final Map<String, List<String>> biggerStringResults,
        final Map<String, List<String>> smallerStringResults,
        final Map<String, List<Double>> biggerDoubleResults,
        final Map<String, List<Double>> smallerDoubleResults, final Row big, final Row small) {
        // Add from big
        for (String colName : biggerStringResults.keySet()) {
            biggerStringResults.get(colName).add(big.getAs(colName));
        }
        for (String colName : biggerDoubleResults.keySet()) {
            biggerDoubleResults.get(colName).add(big.getAs(colName));
        }
        // Add from small
        for (String colName : smallerStringResults.keySet()) {
            smallerStringResults.get(colName).add(small.getAs(colName));
        }
        for (String colName : smallerDoubleResults.keySet()) {
            smallerDoubleResults.get(colName).add(small.getAs(colName));
        }
    }

    /**
     * Extracts the column we're joining on as a single String. We also enforce
     * all of the assumptions we make in {@link #evaluateJoin(Join)}:
     * 1) We join only on a single Column
     * 2) The join must be an equality join
     *
     * @throws MacroBaseSQLException if the assumptions are violated
     */
    private String getJoinColumn(final JoinCriteria joinCriteria,
        Schema biggerSchema, Schema smallerSchema) throws MacroBaseSQLException {
        if (joinCriteria instanceof JoinOn) {
            final JoinOn joinOn = (JoinOn) joinCriteria;
            final Expression joinExpression = joinOn.getExpression();
            if (!(joinExpression instanceof Identifier)) {
                throw new MacroBaseSQLException("Only one column allowed with JOIN ON");
            }
            return ((Identifier) joinExpression).getValue();
        } else if (joinCriteria instanceof JoinUsing) {
            final JoinUsing joinUsing = (JoinUsing) joinCriteria;
            if (joinUsing.getColumns().size() != 1) {
                throw new MacroBaseSQLException("Only one column allowed with JOIN USING");
            }
            return joinUsing.getColumns().get(0).getValue();
        } else if (joinCriteria instanceof NaturalJoin) {
            final List<String> intersection = biggerSchema.getColumnNames().stream()
                .filter(smallerSchema.getColumnNames()::contains).collect(toImmutableList());
            if (intersection.size() != 1) {
                throw new MacroBaseSQLException("Exactly one column allowed with NATURAL JOIN");
            }
            return intersection.get(0);
        } else {
            throw new MacroBaseSQLException(
                "Unsupported join criteria: " + joinCriteria.toString());
        }
    }

    /**
     * TODO
     */
    private BiPredicate<Row, Row> getJoinLambda(final int biggerColIndex, final int smallerColIndex,
        ColType colType) throws MacroBaseSQLException {
        if (colType == ColType.DOUBLE) {
            final BiDoublePredicate lambda = generateBiDoubleLambda(EQUAL);
            return (big, small) -> lambda.test((double) big.getVals().get(biggerColIndex),
                (double) small.getVals().get(smallerColIndex));
        } else {
            // ColType.STRING
            final BiPredicate<String, String> lambda = generateBiStringLambda(EQUAL);
            return (big, small) -> lambda.test((String) big.getVals().get(biggerColIndex),
                (String) small.getVals().get(smallerColIndex));
        }
    }

    /**
     * TODO
     */
    private BiDoublePredicate generateBiDoubleLambda(ComparisonExpressionType compareExprType)
        throws MacroBaseSQLException {
        switch (compareExprType) {
            case EQUAL:
                return (x, y) -> x == y;
            case NOT_EQUAL:
            case IS_DISTINCT_FROM:
                // IS DISTINCT FROM is true when x and y have different values or
                // if one of them is NULL and the other isn't.
                // x and y can never be NULL here, so it's the same as NOT_EQUAL
                return (x, y) -> x != y;
            case LESS_THAN:
                return (x, y) -> x < y;
            case LESS_THAN_OR_EQUAL:
                return (x, y) -> x <= y;
            case GREATER_THAN:
                return (x, y) -> x > y;
            case GREATER_THAN_OR_EQUAL:
                return (x, y) -> x >= y;
            default:
                throw new MacroBaseSQLException(compareExprType + " is not supported");
        }
    }

    /**
     * TODO
     */
    private BiPredicate<String, String> generateBiStringLambda(
        ComparisonExpressionType compareExprType) throws MacroBaseSQLException {
        switch (compareExprType) {
            case EQUAL:
                return Objects::equals;
            case NOT_EQUAL:
            case IS_DISTINCT_FROM:
                // IS DISTINCT FROM is true when x and y have different values or
                // if one of them is NULL and the other isn't
                return (x, y) -> !Objects.equals(x, y);
            case LESS_THAN:
                return (x, y) -> x.compareTo(y) < 0;
            case LESS_THAN_OR_EQUAL:
                return (x, y) -> x.compareTo(y) <= 0;
            case GREATER_THAN:
                return (x, y) -> x.compareTo(y) > 0;
            case GREATER_THAN_OR_EQUAL:
                return (x, y) -> x.compareTo(y) >= 0;
            default:
                throw new MacroBaseSQLException(compareExprType + " is not supported");
        }
    }

    /**
     * TODO
     */
    private DataFrame getDataFrameForRelation(final Relation relation) throws MacroBaseException {
        if (relation instanceof TableSubquery) {
            final QueryBody subquery = ((TableSubquery) relation).getQuery().getQueryBody();
            return executeQuery(subquery);
        } else if (relation instanceof AliasedRelation) {
            return getTable(
                ((Table) ((AliasedRelation) relation).getRelation()).getName().toString());
        } else if (relation instanceof Table) {
            return getTable(((Table) relation).getName().toString());
        } else {
            throw new MacroBaseSQLException("Unsupported relation type");
        }
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
        final DoublePredicate predicate = getPredicate(((DoubleLiteral) val).getValue(), type);
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
                getPredicate(((DoubleLiteral) literal).getValue(), compExprType));
        } else {
            // colType == ColType.STRING
            if (literal instanceof StringLiteral) {
                return df.getMaskForFilter(colIndex,
                    getPredicate(((StringLiteral) literal).getValue(), compExprType));
            } else if (literal instanceof NullLiteral) {
                return df.getMaskForFilter(colIndex, getPredicate(null, compExprType));
            } else {
                throw new MacroBaseSQLException(
                    "Column " + colName + " has type " + colType + ", but " + literal
                        + " is not StringLiteral");
            }
        }
    }

    /**
     * Return a Java Predicate expression for a given comparison type and constant value of type
     * double. (See {@link QueryEngine#getPredicate(String, ComparisonExpressionType)} for handling
     * a String argument.)
     *
     * @param y The constant value
     * @param compareExprType One of =, !=, >, >=, <, <=, or IS DISTINCT FROM
     * @return A {@link DoublePredicate}, that wraps the constant y in a closure
     * @throws MacroBaseSQLException If a comparsion type is passed in that is not supported, an
     * exception is thrown
     */
    private DoublePredicate getPredicate(double y, ComparisonExpressionType compareExprType)
        throws MacroBaseSQLException {
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
     * String. (See {@link QueryEngine#getPredicate(double, ComparisonExpressionType)} for handling
     * a double argument.)
     *
     * @param y The constant value
     * @param compareExprType One of =, !=, >, >=, <, <=, or IS DISTINCT FROM
     * @return A {@link Predicate<Object>}, that wraps the constant y in a closure. A
     * Predicate<String> is not returned for compatibility with {@link DataFrame#filter(int,
     * Predicate)}.
     * @throws MacroBaseSQLException If a comparsion type is passed in that is not supported, an
     * exception is thrown
     */
    private Predicate<String> getPredicate(final String y,
        final ComparisonExpressionType compareExprType) throws MacroBaseSQLException {
        switch (compareExprType) {
            case EQUAL:
                return (x) -> Objects.equals(x, y);
            case NOT_EQUAL:
            case IS_DISTINCT_FROM:
                // IS DISTINCT FROM is true when x and y have different values or
                // if one of them is NULL and the other isn't
                return (x) -> !Objects.equals(x, y);
            case LESS_THAN:
                return (x) -> x.compareTo(y) < 0;
            case LESS_THAN_OR_EQUAL:
                return (x) -> x.compareTo(y) <= 0;
            case GREATER_THAN:
                return (x) -> x.compareTo(y) > 0;
            case GREATER_THAN_OR_EQUAL:
                return (x) -> x.compareTo(y) >= 0;
            default:
                throw new MacroBaseSQLException(compareExprType + " is not supported");
        }
    }
}

