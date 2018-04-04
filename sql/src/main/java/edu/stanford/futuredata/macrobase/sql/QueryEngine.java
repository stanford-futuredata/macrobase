package edu.stanford.futuredata.macrobase.sql;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric.Action.KEEP;
import static edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric.Action.NEXT;
import static edu.stanford.futuredata.macrobase.sql.tree.ComparisonExpressionType.EQUAL;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import edu.stanford.futuredata.macrobase.analysis.MBFunction;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanationResult;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLOutlierSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.util.FastFixedHashTable;
import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSet;
import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSetAsLong;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.GlobalRatioQualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.SupportQualityMetric;
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
import java.util.BitSet;
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
import org.antlr.v4.runtime.misc.Pair;
import org.roaringbitmap.RoaringBitmap;
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
        final double minRatioMetric = diffQuery.getMinRatioExpression().getMinRatio();
        final double minSupport = diffQuery.getMinSupportExpression().getMinSupport();
        final String ratioMetric = diffQuery.getRatioMetricExpr().getFuncName().toString();
        final int order = diffQuery.getMaxCombo().getValue();
        List<String> explainCols = diffQuery.getAttributeCols().stream()
            .map(Identifier::getValue)
            .collect(toImmutableList());

        DataFrame dfToExplain;

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
                return evaluateSQLClauses(diffQuery,
                    evaluateDiffJoin(firstJoin, secondJoin, explainCols, minRatioMetric,
                        minSupport, order));
            }

            // execute subqueries
            final DataFrame outliersDf = executeQuery(first.getQuery().getQueryBody());
            final DataFrame inliersDf = executeQuery(second.getQuery().getQueryBody());

            dfToExplain = concatOutliersAndInliers(outlierColName, outliersDf, inliersDf);
        } else {
            // case 2: single SPLIT (...) WHERE ... query
            final SplitQuery splitQuery = diffQuery.getSplitQuery().get();
            final Relation relationToExplain = splitQuery.getInputRelation();
            dfToExplain = getDataFrameForRelation(relationToExplain);

            // add outlier (binary) column by evaluating the WHERE clause
            final BitSet mask = getMask(dfToExplain, splitQuery.getWhereClause());
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
     * Execute DIFF-JOIN query using co-optimized algorithm. NOTE: Must be a Primary Key-Foreign Key
     * Join. TODO: We make the following assumptions in the method below:
     * 1) The two joins are both inner joins over the same, single column, which is of type String
     * 2) @param explainCols can only be columns in T
     * 3) The ratio metric is global_ratio
     *
     *  R     S          T
     * ---   ---   -------------
     *  a     a     a | CA | v1
     *  a     b     b | CA | v2
     *  b     c     c | TX | v1
     *  b     d     d | TX | v2
     *        e     e | FL | v1
     *
     * @return result of the DIFF JOIN
     */
    private DataFrame evaluateDiffJoin(final Join first, final Join second,
        final List<String> explainColumnNames, final double minRatioMetric, final double minSupport,
        final int maxOrder) throws MacroBaseException {
        final long startTime = System.currentTimeMillis();

        final DataFrame outlierDf = getDataFrameForRelation(first.getLeft()); // table R
        final DataFrame inlierDf = getDataFrameForRelation(second.getLeft()); // table S
        final DataFrame common = getDataFrameForRelation(first.getRight()); // table T

        final Optional<JoinCriteria> joinCriteriaOpt = first.getCriteria();
        if (!joinCriteriaOpt.isPresent()) {
            throw new MacroBaseSQLException("No clause (e.g., ON, USING) specified in JOIN");
        }

        final String joinColumnName = getJoinColumn(joinCriteriaOpt.get(), outlierDf.getSchema(),
            common.getSchema()); // column A1

        final int numOutliers = outlierDf.getNumRows();
        final int numInliers = inlierDf.getNumRows();
        log.info("Num Outliers:  {}, num inliers: {}", numOutliers, numInliers);
        final int minSupportThreshold = (int) minSupport * numOutliers;
        final double globalRatioDenom =
            numOutliers / (numOutliers + numInliers + 0.0);
        final double minRatioThreshold = minRatioMetric * globalRatioDenom;

        // 1a) Encode \proj_{A1} R, \proj_{A1} S, and T
        final String[] outlierProjected = outlierDf.project(joinColumnName).getStringColumn(0);
        final String[] inlierProjected = inlierDf.project(joinColumnName).getStringColumn(0);
        final AttributeEncoder encoder = new AttributeEncoder();
        final int numExplainColumns = explainColumnNames.size();
        final int[][] encodedPrimaryKeyAndValues = new int[common.getNumRows()][
            numExplainColumns + 1]; // include primaryKey column
        final List<String> keyValueColumns = ImmutableList.<String>builder().add(joinColumnName)
            .addAll(explainColumnNames).build();
        final Builder<int[]> encodedForeignKeyBuilder = ImmutableList.builder();
        encoder.encodeKeyValueAttributes(
            ImmutableList.of(outlierProjected, inlierProjected),
            common.getStringColsByName(keyValueColumns),
            encodedForeignKeyBuilder,
            encodedPrimaryKeyAndValues);
        final List<int[]> encodedForeignKeys = encodedForeignKeyBuilder.build();

        // 1) Execute \delta(\proj_{A1} R, \proj_{A1} S);
        final long foreignKeyDiff = System.currentTimeMillis();
        final Map<Integer, Pair<RoaringBitmap, RoaringBitmap>> foreignKeyBitmapPairs = new HashMap<>();
        final Set<Integer> candidateForeignKeys = foreignKeyDiff(
            encodedForeignKeys.get(0), // outlierProjected
            encodedForeignKeys.get(1), // inlierProjected
            foreignKeyBitmapPairs,
            minRatioThreshold); // returns K, the candidate keys that exceeded the minRatioThreshold.
        // K may contain false positives, though (support threshold cannot be applied yet)
        log.info("Foreign key diff time: {} ms", System.currentTimeMillis() - foreignKeyDiff);
        log.info("Num candidate foreign keys: {}", candidateForeignKeys.size());

        // 2) Execute K \semijoin T, to get V, the values in T associated with the candidate keys,
        //    and merge common values that distinct keys may map to

        // Keep track of candidates in each column, needed for order-2 and order-3 combinations
        final long semiJoinAndMergeTime = System.currentTimeMillis();
        final Set<Integer>[] attrCandidatesByColumn = new Set[numExplainColumns];
        for (int i = 0; i < numExplainColumns; ++i) {
            attrCandidatesByColumn[i] = new HashSet<>();
        }
        final Map<Integer, Pair<RoaringBitmap, RoaringBitmap>> valueBitmapPairs = semiJoinAndMerge(
            candidateForeignKeys, // K
            encodedPrimaryKeyAndValues, // T
            foreignKeyBitmapPairs,
            attrCandidatesByColumn);
        log.info("Semi-join and merge time: {} ms", System.currentTimeMillis() - semiJoinAndMergeTime);

        // 3) Prune anything that doesn't have enough support
        //    (End of order-1 step of candidate generation)
        final int sizeBefore = valueBitmapPairs.size();
        valueBitmapPairs.entrySet().removeIf((entry) ->
            entry.getValue().a.getCardinality() < minSupportThreshold);
        log.info("Num candidates pruned by support: {}", valueBitmapPairs.size() - sizeBefore);

        // 4) Now, we proceed as we would in the all-bitmap case of APrioriLinear: finish up the order-1 stage,
        //    then explore all 2-order and 3-order combinations by intersecting the bitmaps, then pruning
        //    using the QualityMetrics
        final int numAggregates = 2;
        final double[] thresholds = new double[]{minSupport, minRatioMetric};
        final QualityMetric[] qualityMetrics = new QualityMetric[]{new SupportQualityMetric(0),
            new GlobalRatioQualityMetric(0, 1)};

        // Quality metrics are initialized with global aggregates to
        // allow them to determine the appropriate relative thresholds
        double[] globalAggregates = new double[]{numOutliers, numOutliers + numInliers};
        for (QualityMetric q : qualityMetrics) {
            q.initialize(globalAggregates);
        }

        // Prune all the collected aggregates
        // Sets that have high enough support but not high qualityMetrics, need to be explored
        final Map<Integer, HashSet<IntSet>> setNext = new HashMap<>(3);
        // Aggregate values for all of the sets we saved
        final Map<Integer, Map<IntSet, double []>> savedAggregates = new HashMap<>(3);

        for (int order = 1; order <= maxOrder; ++order) {
            final long orderTime = System.currentTimeMillis();
            // For now, we always use IntSetAsLong
            final FastFixedHashTable setAggregates = new FastFixedHashTable(order == 1 ?
                encoder.getNextKey() : encoder.getNextKey() * setNext.get(order).size(),
                numAggregates, false);
            if (order == 1) {
                valueBitmapPairs.forEach((key, bitmapPair) -> {
                    final int numMatchedOutliers = bitmapPair.a.getCardinality();
                    final int numMatchedInliers = bitmapPair.b.getCardinality();
                    updateAggregates(setAggregates, new IntSetAsLong(key),
                        new double[]{numMatchedOutliers, numMatchedOutliers + numMatchedInliers},
                        numAggregates);
                });
            } else if (order == 2) {
                IntSetAsLong curCandidate = new IntSetAsLong(0);
                for (int colNumOne = 0; colNumOne < numExplainColumns; ++colNumOne) {
                    for (int colNumTwo = colNumOne + 1; colNumTwo < numExplainColumns;
                        ++colNumTwo) {
                        for (Integer curCandidateOne : attrCandidatesByColumn[colNumOne]) {
                            for (Integer curCandidateTwo : attrCandidatesByColumn[colNumTwo]) {
                                curCandidate.value = IntSetAsLong
                                    .twoIntToLong(curCandidateOne, curCandidateTwo);
                                final Pair<RoaringBitmap, RoaringBitmap> candidateOneBitmapPair = valueBitmapPairs
                                    .get(curCandidateOne);
                                final Pair<RoaringBitmap, RoaringBitmap> candidateTwoBitmapPair = valueBitmapPairs
                                    .get(curCandidateTwo);
                                final int numMatchedOutliers = RoaringBitmap
                                    .andCardinality(candidateOneBitmapPair.a,
                                        candidateTwoBitmapPair.a);
                                final int numMatchedInliers = RoaringBitmap
                                    .andCardinality(candidateOneBitmapPair.b,
                                        candidateTwoBitmapPair.b);
                                updateAggregates(setAggregates, curCandidate,
                                    new double[]{numMatchedOutliers, numMatchedOutliers + numMatchedInliers},
                                    numAggregates);
                            }
                        }
                    }
                }
            } else if (order == 3) {
                IntSetAsLong curCandidate = new IntSetAsLong(0);
                for (int colNumOne = 0; colNumOne < numExplainColumns; ++colNumOne) {
                    for (int colNumTwo = colNumOne + 1; colNumTwo < numExplainColumns;
                        ++colNumTwo) {
                        for (int colNumThree = colNumTwo + 1; colNumThree < numExplainColumns;
                            ++colNumThree) {
                            for (Integer curCandidateOne : attrCandidatesByColumn[colNumOne]) {
                                for (Integer curCandidateTwo : attrCandidatesByColumn[colNumTwo]) {
                                    for (Integer curCandidateThree : attrCandidatesByColumn[colNumThree]) {
                                        // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                                        curCandidate.value = IntSetAsLong
                                            .threeIntToLong(curCandidateOne, curCandidateTwo,
                                                curCandidateThree);
                                        final Pair<RoaringBitmap, RoaringBitmap> candidateOneBitmapPair = valueBitmapPairs
                                            .get(curCandidateOne);
                                        final Pair<RoaringBitmap, RoaringBitmap> candidateTwoBitmapPair = valueBitmapPairs
                                            .get(curCandidateTwo);
                                        final Pair<RoaringBitmap, RoaringBitmap> candidateThreeBitmapPair = valueBitmapPairs
                                            .get(curCandidateThree);
                                        // TODO: write three-argument version of andCardinality
                                        final int numMatchedOutliers = RoaringBitmap.andCardinality(
                                            RoaringBitmap.and(candidateOneBitmapPair.a,
                                                candidateTwoBitmapPair.a),
                                            candidateThreeBitmapPair.a);
                                        final int numMatchedInliers = RoaringBitmap.andCardinality(
                                            RoaringBitmap.and(candidateOneBitmapPair.b,
                                                candidateTwoBitmapPair.b),
                                            candidateThreeBitmapPair.b);
                                        updateAggregates(setAggregates, curCandidate,
                                            new double[]{numMatchedOutliers,
                                                numMatchedOutliers + numMatchedInliers},
                                            numAggregates);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            HashSet<IntSet> curOrderNext = new HashSet<>();
            HashSet<IntSet> curOrderSaved = new HashSet<>();
            for (long l : setAggregates.keySetLong()) {
                final IntSetAsLong candidate = new IntSetAsLong(l);
                QualityMetric.Action action = KEEP;
                double[] curAggregates = setAggregates.get(candidate);
                for (int i = 0; i < qualityMetrics.length; i++) {
                    QualityMetric q = qualityMetrics[i];
                    double t = thresholds[i];
                    action = QualityMetric.Action.combine(action, q.getAction(curAggregates, t));
                }
                if (action == KEEP) {
                    // Make sure the candidate isn't already covered by a pair
                    if (order != 3 || allPairsValid(candidate, setNext.get(2))) {
                        // if a set is already past the threshold on all metrics,
                        // save it and no need for further exploration if we do containment
                        curOrderSaved.add(candidate);
                        // Remove this explanation from the array of candidate sets
                        switch (order) {
                            case 2:
                                int val = candidate.getSecond();
                                // subtract three for foreign key columns and primary key column
                                attrCandidatesByColumn[encoder.decodeColumn(val) - 3].remove(val);
                            case 1:
                                val = candidate.getFirst();
                                attrCandidatesByColumn[encoder.decodeColumn(val) - 3].remove(val);
                        }
                    }
                } else if (action == NEXT) {
                    // If a set still has potentially good subsets, save it for further examination
                    curOrderNext.add(candidate);
                } else {
                    // Prune search space by removing candidates from attrCandidatesByColumn
                    switch (order) {
                        case 3:
                            int val = candidate.getThird();
                            attrCandidatesByColumn[encoder.decodeColumn(val) - 3].remove(val);
                        case 2:
                            val = candidate.getSecond();
                            attrCandidatesByColumn[encoder.decodeColumn(val) - 3].remove(val);
                        case 1:
                            val = candidate.getFirst();
                            attrCandidatesByColumn[encoder.decodeColumn(val) - 3].remove(val);
                            break;
                    }
                }
            }

            // Save aggregates that pass all qualityMetrics to return later, store aggregates
            // that have minimum support for higher-order exploration.
            Map<IntSet, double[]> curSavedAggregates = new HashMap<>(curOrderSaved.size());
            for (IntSet curSaved : curOrderSaved) {
                curSavedAggregates.put(curSaved, setAggregates.get(curSaved));
            }
            savedAggregates.put(order, curSavedAggregates);
            if (curOrderNext.isEmpty()) {
                log.info("curOrderNext is empty, skipping higher-order explanations");
                break;
            }
            setNext.put(order, curOrderNext);
            log.info("Order {} time: {} ms", order, System.currentTimeMillis() - orderTime);
        }
        log.info("Time spent in DiffJoin: {} ms", System.currentTimeMillis() - startTime);

        // 4) Construct DataFrame of results
        List<APLExplanationResult> results = new ArrayList<>();
        for (Map<IntSet, double []> curOrderSavedAggregates : savedAggregates.values()) {
            if (curOrderSavedAggregates == null) {
                // we stopped early, because there was nothing in curOrderNext
                continue;
            }
            for (Entry<IntSet, double[]> entry : curOrderSavedAggregates.entrySet()) {
                double[] aggregates = entry.getValue();
                double[] metrics = new double[qualityMetrics.length];
                for (int i = 0; i < metrics.length; i++) {
                    metrics[i] = qualityMetrics[i].value(aggregates);
                }
                results.add(
                    new APLExplanationResult(qualityMetrics, entry.getKey(), aggregates, metrics)
                );
            }
        }

        return toDataFrame(results, encoder, explainColumnNames, keyValueColumns,
        ImmutableList.of("outlier_count", "total_count"), Arrays.asList(qualityMetrics));
    }

    private void updateAggregates(FastFixedHashTable aggregates, IntSet curCandidate, double[] val, int numAggregates) {
        double[] candidateVal = aggregates.get(curCandidate);
        if (candidateVal == null) {
            aggregates.put(curCandidate,
                Arrays.copyOf(val, numAggregates));
        } else {
            for (int a = 0; a < numAggregates; a++) {
                candidateVal[a] += val[a];
            }
        }
    }

    /**
     * Check if all order-2 subsets of an order-3 candidate are valid candidates.
     * @param o2Candidates All candidates of order 2 with minimum support.
     * @param curCandidate An order-3 candidate
     * @return Boolean
     */
    private boolean allPairsValid(IntSet curCandidate,
        HashSet<IntSet> o2Candidates) {
        IntSet subPair;
        subPair = new IntSetAsLong(
            curCandidate.getFirst(),
            curCandidate.getSecond());
        if (o2Candidates.contains(subPair)) {
            subPair = new IntSetAsLong(
                curCandidate.getSecond(),
                curCandidate.getThird());
            if (o2Candidates.contains(subPair)) {
                subPair = new IntSetAsLong(
                    curCandidate.getFirst(),
                    curCandidate.getThird());
                return o2Candidates.contains(subPair);
            }
        }
        return false;
    }

    private DataFrame toDataFrame(final List<APLExplanationResult> results,
        final AttributeEncoder encoder, final List<String> attrsToInclude, final List<String> keyValueColumns,
        final List<String> aggregateNames, final List<QualityMetric> metrics) {
        // String column values that will be added to DataFrame
        final Map<String, String[]> stringResultsByCol = new HashMap<>();
        for (String colName : attrsToInclude) {
            stringResultsByCol.put(colName, new String[results.size()]);
        }

        // double column values that will be added to the DataFrame
        final Map<String, double[]> doubleResultsByCol = new HashMap<>();
        for (String colName : aggregateNames) {
            doubleResultsByCol.put(colName, new double[results.size()]);
        }
        for (QualityMetric metric : metrics) {
            // NOTE: we assume that the QualityMetrics here are the same ones
            // that each APLExplanationResult has
            doubleResultsByCol.put(metric.name(), new double[results.size()]);
        }

        // Add result rows to individual columns
        int i = 0;
        for (APLExplanationResult result : results) {
            // attrValsInRow contains the values for the explanation attribute values in this
            // given row
            Set<Integer> values = result.matcher.getSet();
            final Map<String, String> attrValsInRow = new HashMap<>();
            for (int v : values) {
                final String colNameForValue = keyValueColumns
                    .get(encoder.decodeColumn(v) - 2); // skip the foreign key columns
                attrValsInRow.put(colNameForValue, encoder.decodeValue(v));
            }

            for (String colName : stringResultsByCol.keySet()) {
                // Iterate over all attributes that will be in the DataFrame.
                // If attribute is present in attrValsInRow, add its corresponding value.
                // Otherwise, add null
                stringResultsByCol.get(colName)[i] = attrValsInRow.get(colName);
            }

            final Map<String, Double> metricValsInRow = result.getMetricsAsMap();
            for (String colName : metricValsInRow.keySet()) {
                doubleResultsByCol.get(colName)[i] = metricValsInRow.get(colName);
            }

            final Map<String, Double> aggregateValsInRow = result
                .getAggregatesAsMap(aggregateNames);
            for (String colName : aggregateNames) {
                doubleResultsByCol.get(colName)[i] = aggregateValsInRow.get(colName);
            }
            ++i;
        }

        // Generate DataFrame with results
        final DataFrame df = new DataFrame();
        for (String colName : stringResultsByCol.keySet()) {
            df.addColumn(colName, stringResultsByCol.get(colName));
        }
        // Add metrics first, then aggregates (otherwise, we'll get arbitrary orderings)
        for (QualityMetric metric : metrics) {
            df.addColumn(metric.name(), doubleResultsByCol.get(metric.name()));
        }
        for (String colName : aggregateNames) {
            // Aggregates are capitalized for some reason
            df.addColumn(colName.toLowerCase(), doubleResultsByCol.get(colName));
        }
        return df;
    }

    private Map<Integer, Pair<RoaringBitmap, RoaringBitmap>> semiJoinAndMerge(
        final Set<Integer> candidateForeignKeys, final int[][] encodedValues,
        final Map<Integer, Pair<RoaringBitmap, RoaringBitmap>> foreignKeyBitmapPairs,
        Set<Integer>[] attrCandidatesByColumn) {

        int numAdditionalValues = 0;

        final Map<Integer, Pair<RoaringBitmap, RoaringBitmap>> valueBitmapPairs = new HashMap<>();
        // 1) R \semijoin T: Go through the primary key column and see what candidateForeignKeys are contained.
        //    For every match, save the corresponding values
        final int numCols = encodedValues[0].length;
        for (int[] encodedValue : encodedValues) {
            final int primaryKey = encodedValue[0];
            if (candidateForeignKeys.contains(primaryKey)) {
                final Pair<RoaringBitmap, RoaringBitmap> foreignKeyBitmapPair = foreignKeyBitmapPairs
                    .get(primaryKey); // this always exists, never need to check for null
                // extract the corresponding values for the candidate key
                for (int j = 1; j < numCols; ++j) {
                    final int val = encodedValue[j];
                    final Pair<RoaringBitmap, RoaringBitmap> valueBitmapPair = valueBitmapPairs
                        .get(val);
                    if (valueBitmapPair == null) {
                        valueBitmapPairs.put(val, new Pair<>(foreignKeyBitmapPair.a.clone(),
                            foreignKeyBitmapPair.b.clone()));
                        attrCandidatesByColumn[j - 1].add(val); // subtract one for primary key column
                    } else {
                        // if the value already exists, merge the foreign key bitmaps
                        valueBitmapPair.a.or(foreignKeyBitmapPair.a); // outliers
                        valueBitmapPair.b.or(foreignKeyBitmapPair.b); // inliers
                        ++numAdditionalValues;
                    }
                }
            }
        }
        // 2) Go through again and check which saved values from the first pass map to new
        //    primary keys. If we find any new ones, merge their foreign key bitmaps
        //    with the existing value bitmap
        for (int[] encodedValue : encodedValues) {
            for (int j = 1; j < numCols; ++j) {
                final int val = encodedValue[j];
                final Pair<RoaringBitmap, RoaringBitmap> valueBitmapPair = valueBitmapPairs
                    .get(val);
                if (valueBitmapPair == null) {
                    // never found in the first pass
                    continue;
                }
                // extract the corresponding foreign key, merge the foreign key bitmaps
                final int primaryKey = encodedValue[0];
                if (candidateForeignKeys.contains(primaryKey)) {
                    // found in the first pass, but already included in valueBitmapPair
                    continue;
                }
                final Pair<RoaringBitmap, RoaringBitmap> foreignKeyBitmapPair = foreignKeyBitmapPairs
                    .get(primaryKey);
                if (foreignKeyBitmapPair != null) {
                    valueBitmapPair.a.or(foreignKeyBitmapPair.a); // outliers
                    valueBitmapPair.b.or(foreignKeyBitmapPair.b); // inliers
                    ++numAdditionalValues;
                }
            }
        }
        log.info("Num additional values: {}", numAdditionalValues);
        return valueBitmapPairs;
    }

    private Set<Integer> foreignKeyDiff(final int[] outliers, final int[] inliers,
        final Map<Integer, Pair<RoaringBitmap, RoaringBitmap>> foreignKeyCounts,
        double minRatioThreshold) {
        for (int i = 0; i < outliers.length; ++i) {
            final int outlier = outliers[i];
            final Pair<RoaringBitmap, RoaringBitmap> value = foreignKeyCounts.get(outlier);
            if (value != null) {
                value.a.add(i);
            } else {
                foreignKeyCounts
                    .put(outlier, new Pair<>(RoaringBitmap.bitmapOf(i), new RoaringBitmap()));
            }
        }
        for (int i = 0; i < inliers.length; ++i) {
            final int inlier = inliers[i];
            final Pair<RoaringBitmap, RoaringBitmap> value = foreignKeyCounts.get(inlier);
            if (value != null) {
                value.b.add(i);
            } else {
                foreignKeyCounts
                    .put(inlier, new Pair<>(new RoaringBitmap(), RoaringBitmap.bitmapOf(i)));
            }
        }
        final ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
        for (Entry<Integer, Pair<RoaringBitmap, RoaringBitmap>> entry : foreignKeyCounts
            .entrySet()) {
            final Pair<RoaringBitmap, RoaringBitmap> value = entry.getValue();
            final int numOutliers = value.a.getCardinality();
            if ((numOutliers / (numOutliers + value.b.getCardinality() + 0.0))
                >= minRatioThreshold) {
                builder.add(entry.getKey());
            }
        }
        return builder.build();
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
    private DataFrame evaluateJoin(Join join) throws MacroBaseException  {
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

                BiPredicate<Row, Row> lambda = getJoinLambda(biggerColIndex, smallerColIndex,
                    biggerColType);
                for (Row big : bigger.getRowIterator()) {
                    for (Row small : smaller.getRowIterator()) {
                        if (lambda.test(big, small)) {
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
                    }
                }
                log.info("Time spent in Join:  {} ms", System.currentTimeMillis() - startTime);

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
            default:
                throw new MacroBaseSQLException("Join type " + join.getType() + "not supported");
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
        final BitSet mask = getMask(df, whereClause);
        return df.filter(mask);
    }

    // ********************* Helper methods for evaluating Where clauses **********************

    /**
     * Recursive method that, given a Where clause, generates a boolean mask (a BitSet) applying the
     * clause to a DataFrame
     *
     * @throws MacroBaseSQLException Only comparison expressions (e.g., WHERE x = 42) and logical
     * AND/OR/NOT combinations of such expressions are supported; exception is thrown otherwise.
     */
    private BitSet getMask(DataFrame df, Expression whereClause) throws MacroBaseException {
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
        throw new MacroBaseSQLException("Boolean expression not supported");
    }

    private BitSet maskForPredicate(DataFrame df, FunctionCall func, Literal val,
        final ComparisonExpressionType type)
        throws MacroBaseException {
        final String funcName = func.getName().getSuffix();
        final MBFunction mbFunction = MBFunction.getFunction(funcName,
            func.getArguments().stream().map(Expression::toString).findFirst().get());
        final double[] col = mbFunction.apply(df);
        final DoublePredicate predicate = getPredicate(((DoubleLiteral) val).getValue(), type);
        final BitSet mask = new BitSet(col.length);
        for (int i = 0; i < col.length; ++i) {
            if (predicate.test(col[i])) {
                mask.set(i);
            }
        }
        return mask;
    }


    /**
     * The base case for {@link QueryEngine#getMask(DataFrame, Expression)}; returns a boolean mask
     * (as a BitSet) for a single comparision expression (e.g., WHERE x = 42)
     *
     * @param df The DataFrame on which to evaluate the comparison expression
     * @param literal The constant argument in the expression (e.g., 42)
     * @param identifier The column variable argument in the expression (e.g., x)
     * @param compExprType One of =, !=, >, >=, <, <=, or IS DISTINCT FROM
     * @throws MacroBaseSQLException if the literal's type doesn't match the type of the column
     * variable, an exception is thrown
     */
    private BitSet maskForPredicate(final DataFrame df, final Literal literal,
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

