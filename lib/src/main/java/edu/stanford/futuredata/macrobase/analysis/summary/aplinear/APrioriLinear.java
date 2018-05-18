package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.*;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.util.MacroBaseInternalError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.roaringbitmap.RoaringBitmap;
import org.w3c.dom.Attr;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static edu.stanford.futuredata.macrobase.analysis.summary.aplinear.BitmapHelperFunctions.*;

/**
 * Class for handling the generic, algorithmic aspects of apriori explanation.
 * This class assumes that subgroups posses "aggregates" such as count and outlier_count
 * which can be combined additively. Then, we use APriori to find the subgroups which
 * are the most interesting as defined by "quality metrics" on these aggregates.
 */
public class APrioriLinear {
    private Logger log = LoggerFactory.getLogger("APrioriLinear");

    // **Parameters**
    private QualityMetric[] qualityMetrics;
    private double[] thresholds;

    // **Cached values**
    // Sets that have high enough support but not high qualityMetrics, need to be explored
    private HashMap<Integer, HashSet<IntSet>> setNext;
    // Aggregate values for all of the sets we saved
    private HashMap<Integer, Map<IntSet, double []>> savedAggregates;

    public APrioriLinear(
            List<QualityMetric> qualityMetrics,
            List<Double> thresholds
    ) {
        this.qualityMetrics = qualityMetrics.toArray(new QualityMetric[0]);
        this.thresholds = new double[thresholds.size()];
        for (int i = 0; i < thresholds.size(); i++) {
            this.thresholds[i] = thresholds.get(i);
        }
        this.setNext = new HashMap<>(3);
        this.savedAggregates = new HashMap<>(3);
    }

    public List<APLExplanationResult> explain(
            final int[][] attributes,
            double[][] aggregateColumns,
            AggregationOp[] aggregationOps,
            int cardinality,
            final int maxOrder,
            int numThreads,
            HashMap<Integer, ModBitSet>[][] bitmap,
            ArrayList<Integer>[] outlierList,
            int[] colCardinalities,
            boolean useFDs,
            int[] functionalDependencies,
            int bitmapRatioThreshold
    ) {
        final int numAggregates = aggregateColumns.length;
        final int numRows = aggregateColumns[0].length;
        final int numColumns = attributes[0].length;

        // Singleton viable sets for quick lookup
        boolean[] singleNextArray = new boolean[cardinality];;

        // Maximum order of explanations.
        final boolean useIntSetAsArray;
        // 2097151 is 2^21 - 1, the largest value that can fit in a length-three IntSetAsLong.
        // If the cardinality is greater than that, don't use them.
        if (cardinality >= 2097151) {
            log.warn("Cardinality is extremely high.  Candidate generation will be slow.");
            useIntSetAsArray = true;
        } else{
            useIntSetAsArray = false;
        }
        log.info("NumThreads: {}", numThreads);
        // Shard the dataset by rows for the threads, but store it by column for fast processing
        final int[][][] byThreadAttributesTranspose =
                new int[numThreads][numColumns][(numRows + numThreads)/numThreads];
        final HashMap<Integer, ModBitSet>[][][] byThreadBitmap = new HashMap[numThreads][numColumns][2];
        for (int i = 0; i < numThreads; i++)
            for (int j = 0; j < numColumns; j++)
                for (int k = 0; k < 2; k++)
                    byThreadBitmap[i][j][k] = new HashMap<>();
        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            final int startIndex = (numRows * threadNum) / numThreads;
            final int endIndex = (numRows * (threadNum + 1)) / numThreads;
            for(int i = 0; i < numColumns; i++) {
                for (int j = startIndex; j < endIndex; j++) {
                    byThreadAttributesTranspose[threadNum][i][j - startIndex] = attributes[j][i];
                }
                if (colCardinalities[i] < AttributeEncoder.cardinalityThreshold) {
                    for (int j = 0; j < 2; j++) {
                        for (HashMap.Entry<Integer, ModBitSet> entry : bitmap[i][j].entrySet()) {
                            ModBitSet rr = entry.getValue().get(startIndex, endIndex);
                            if (rr.cardinality() > 0) {
                                byThreadBitmap[threadNum][i][j].put(entry.getKey(), rr);
                            }
                        }
                    }
                }
            }
        }

        // Quality metrics are initialized with global aggregates to
        // allow them to determine the appropriate relative thresholds
        double[] globalAggregates = new double[numAggregates];
        for (int j = 0; j < numAggregates; j++) {
            AggregationOp curOp = aggregationOps[j];
            globalAggregates[j] = curOp.initValue();
            double[] curColumn = aggregateColumns[j];
            for (int i = 0; i < numRows; i++) {
                globalAggregates[j] = curOp.combine(globalAggregates[j], curColumn[i]);
            }
        }
        for (QualityMetric q : qualityMetrics) {
            q.initialize(globalAggregates);
        }

        // Row store for more convenient access
        final double[][] aRows = new double[numRows][numAggregates];
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numAggregates; j++) {
                aRows[i][j] = aggregateColumns[j][i];
            }
        }
        for (int curOrder = 1; curOrder <= maxOrder; curOrder++) {
            long startTime = System.currentTimeMillis();
            final int curOrderFinal = curOrder;
            // Initialize per-thread hashmaps.
            final ArrayList<FastFixedHashTable> threadSetAggregates = new ArrayList<>(numThreads);
            for (int i = 0; i < numThreads; i++) {
                threadSetAggregates.add(new FastFixedHashTable(cardinality, numAggregates, useIntSetAsArray));
            }
            // Shard the dataset by row into threads and generate candidates.
            final CountDownLatch doneSignal = new CountDownLatch(numThreads);
            for (int threadNum = 0; threadNum < numThreads; threadNum++) {
                final int curThreadNum = threadNum;
                final int startIndex = (numRows * threadNum) / numThreads;
                final int endIndex = (numRows * (threadNum + 1)) / numThreads;
                final FastFixedHashTable thisThreadSetAggregates = threadSetAggregates.get(threadNum);
                // Do candidate generation in a lambda.
                Runnable APrioriLinearRunnable = () -> {
                    IntSet curCandidate;
                    if (!useIntSetAsArray)
                        curCandidate = new IntSetAsLong(0);
                    else
                        curCandidate = new IntSetAsArray(0);
                    if (curOrderFinal == 1) {
                        for (int colNum = 0; colNum < numColumns; colNum++) {
                            if (colCardinalities[colNum] < AttributeEncoder.cardinalityThreshold) {
                                for (Integer curOutlierCandidate : outlierList[colNum]) {
                                    // Require that all order-one candidates have minimum support.
                                    if (curOutlierCandidate == AttributeEncoder.noSupport)
                                        continue;
                                    int outlierCount = 0, inlierCount = 0;
                                    if (byThreadBitmap[curThreadNum][colNum][1].containsKey(curOutlierCandidate))
                                        outlierCount = byThreadBitmap[curThreadNum][colNum][1].get(curOutlierCandidate).cardinality();
                                    if (byThreadBitmap[curThreadNum][colNum][0].containsKey(curOutlierCandidate))
                                        inlierCount = byThreadBitmap[curThreadNum][colNum][0].get(curOutlierCandidate).cardinality();
                                    // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                                    if (useIntSetAsArray) {
                                        curCandidate = new IntSetAsArray(curOutlierCandidate);
                                    } else {
                                        ((IntSetAsLong) curCandidate).value = curOutlierCandidate;
                                    }
                                    updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps,
                                            new double[]{outlierCount, outlierCount + inlierCount}, numAggregates);
                                }
                            } else {
                                int[] curColumnAttributes = byThreadAttributesTranspose[curThreadNum][colNum];
                                for (int rowNum = startIndex; rowNum < endIndex; rowNum++) {
                                    // Require that all order-one candidates have minimum support.
                                    if (curColumnAttributes[rowNum - startIndex] == AttributeEncoder.noSupport)
                                        continue;
                                    // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                                    if (useIntSetAsArray) {
                                        curCandidate = new IntSetAsArray(curColumnAttributes[rowNum - startIndex]);
                                    } else {
                                        ((IntSetAsLong) curCandidate).value = curColumnAttributes[rowNum - startIndex];
                                    }
                                    updateAggregates(thisThreadSetAggregates, curCandidate, aggregationOps,
                                            aRows[rowNum], numAggregates);
                                }
                            }
                        }
                    } else if (curOrderFinal == 2) {
                        for (int colNumOne = 0; colNumOne < numColumns; colNumOne++) {
                            int[] curColumnOneAttributes = byThreadAttributesTranspose[curThreadNum][colNumOne];
                            for (int colNumTwo = colNumOne + 1; colNumTwo < numColumns; colNumTwo++) {
                                //if FDs are enabled, and these two attribute cols are FDs, skip
                                if (useFDs && ((functionalDependencies[colNumOne] & (1<<colNumTwo)) == (1<<colNumTwo))) {
                                    continue;
                                }
                                int[] curColumnTwoAttributes = byThreadAttributesTranspose[curThreadNum][colNumTwo];
                                if (colCardinalities[colNumOne] < AttributeEncoder.cardinalityThreshold &&
                                        colCardinalities[colNumOne] < AttributeEncoder.cardinalityThreshold &&
                                        colCardinalities[colNumOne] * colCardinalities[colNumTwo] < bitmapRatioThreshold) {
                                    // Bitmap-Bitmap
                                    allTwoBitmap(thisThreadSetAggregates, outlierList, aggregationOps, singleNextArray,
                                            byThreadBitmap[curThreadNum], colNumOne, colNumTwo, useIntSetAsArray,
                                            curCandidate, numAggregates);
                                }  else {
                                    // Normal-Normal
                                    allTwoNormal(thisThreadSetAggregates, curColumnOneAttributes,
                                            curColumnTwoAttributes, aggregationOps, singleNextArray,
                                            startIndex, endIndex, useIntSetAsArray, curCandidate, aRows,
                                            numAggregates);
                                }
                            }
                        }
                    } else if (curOrderFinal == 3) {
                        // order-3 case
                        for (int colNumOne = 0; colNumOne < numColumns; colNumOne++) {
                            int[] curColumnOneAttributes = byThreadAttributesTranspose[curThreadNum][colNumOne % numColumns];
                            for (int colNumTwo = colNumOne + 1; colNumTwo < numColumns; colNumTwo++) {
                                //if FD on and attributes 1 and 2 are FDs, skip
                                if (useFDs && ((functionalDependencies[colNumOne] & (1<<colNumTwo)) == (1<<colNumTwo))) {
                                    continue;
                                }
                                int[] curColumnTwoAttributes = byThreadAttributesTranspose[curThreadNum][colNumTwo % numColumns];
                                for (int colNumThree = colNumTwo + 1; colNumThree < numColumns; colNumThree++) {
                                    //if FD on and attribute 3 is FD w/ 1 or 2, skip
                                    if (useFDs && (((functionalDependencies[colNumOne] & (1 << colNumThree)) == (1 << colNumThree))
                                            || ((functionalDependencies[colNumTwo] & (1 << colNumThree)) == (1 << colNumThree)))) {
                                        continue;
                                    }
                                    int[] curColumnThreeAttributes = byThreadAttributesTranspose[curThreadNum][colNumThree % numColumns];
                                    if (colCardinalities[colNumOne] < AttributeEncoder.cardinalityThreshold &&
                                            colCardinalities[colNumOne] < AttributeEncoder.cardinalityThreshold &&
                                            colCardinalities[colNumThree] < AttributeEncoder.cardinalityThreshold &&
                                            colCardinalities[colNumOne] * colCardinalities[colNumTwo] *
                                                    colCardinalities[colNumThree] < bitmapRatioThreshold) {
                                        // all 3 cols are bitmaps
                                        allThreeBitmap(thisThreadSetAggregates, outlierList, aggregationOps,
                                                singleNextArray, byThreadBitmap[curThreadNum],
                                                colNumOne, colNumTwo, colNumThree, useIntSetAsArray, curCandidate,
                                                numAggregates);

                                    } else {
                                        // all three are normal
                                        allThreeNormal(thisThreadSetAggregates, curColumnOneAttributes,
                                                curColumnTwoAttributes, curColumnThreeAttributes,
                                                aggregationOps, singleNextArray, startIndex, endIndex,
                                                useIntSetAsArray, curCandidate, aRows, numAggregates);
                                    }
                                }
                            }
                        }
                    } else {
                        throw new MacroBaseInternalError("High Order not supported");
                    }
                    doneSignal.countDown();
                };
                // Run numThreads lambdas in separate threads
                Thread APrioriLinearThread = new Thread(APrioriLinearRunnable);
                APrioriLinearThread.start();
            }
            // Wait for all threads to finish running.
            try {
                doneSignal.await();
            } catch (InterruptedException ex) {ex.printStackTrace();}


            Map<IntSet, double []> setAggregates = new HashMap<>();
            // Collect the aggregates stored in the per-thread HashMaps.
            for (FastFixedHashTable set : threadSetAggregates) {
                if (useIntSetAsArray) {
                    for (IntSet curCandidateKey : set.keySet()) {
                        double[] curCandidateValue = set.get(curCandidateKey);
                        double[] candidateVal = setAggregates.get(curCandidateKey);
                        if (candidateVal == null) {
                            setAggregates.put(curCandidateKey, Arrays.copyOf(curCandidateValue, numAggregates));
                        } else {
                            for (int a = 0; a < numAggregates; a++) {
                                AggregationOp curOp = aggregationOps[a];
                                candidateVal[a] = curOp.combine(candidateVal[a], curCandidateValue[a]);
                            }
                        }
                    }
                } else {
                    for (long curCandidateKeyLong : set.keySetLong()) {
                        IntSetAsLong curCandidateKeyIntSetAsLong = new IntSetAsLong(curCandidateKeyLong);
                        IntSet curCandidateKey = new IntSetAsArray(curCandidateKeyIntSetAsLong);
                        double[] curCandidateValue = set.get(curCandidateKeyIntSetAsLong);
                        double[] candidateVal = setAggregates.get(curCandidateKey);
                        if (candidateVal == null) {
                            setAggregates.put(curCandidateKey, Arrays.copyOf(curCandidateValue, numAggregates));
                        } else {
                            for (int a = 0; a < numAggregates; a++) {
                                AggregationOp curOp = aggregationOps[a];
                                candidateVal[a] = curOp.combine(candidateVal[a], curCandidateValue[a]);
                            }
                        }
                    }
                }
            }

            // Prune all the collected aggregates
            HashSet<IntSet> curOrderNext = new HashSet<>();
            HashSet<IntSet> curOrderSaved = new HashSet<>();
            for (IntSet curCandidate: setAggregates.keySet()) {
                QualityMetric.Action action = QualityMetric.Action.KEEP;
                if (curOrder == 1 && curCandidate.getFirst() == AttributeEncoder.noSupport) {
                    action = QualityMetric.Action.PRUNE;
                } else {
                    double[] curAggregates = setAggregates.get(curCandidate);
                    for (int i = 0; i < qualityMetrics.length; i++) {
                        QualityMetric q = qualityMetrics[i];
                        double t = thresholds[i];
                        action = QualityMetric.Action.combine(action, q.getAction(curAggregates, t));
                    }
                    if (action == QualityMetric.Action.KEEP) {
                        // Make sure the candidate isn't already covered by a pair
                        if (curOrder != 3 || allPairsValid(curCandidate, setNext.get(2))) {
                            // if a set is already past the threshold on all metrics,
                            // save it and no need for further exploration if we do containment
                            curOrderSaved.add(curCandidate);
                            curOrderNext.add(curCandidate);
                        }
                    } else if (action == QualityMetric.Action.NEXT) {
                        // otherwise if a set still has potentially good subsets,
                        // save it for further examination
                        curOrderNext.add(curCandidate);
                    }
                }
            }

            // Save aggregates that pass all qualityMetrics to return later, store aggregates
            // that have minimum support for higher-order exploration.
            Map<IntSet, double []> curSavedAggregates = new HashMap<>(curOrderSaved.size());
            for (IntSet curSaved : curOrderSaved) {
                curSavedAggregates.put(curSaved, setAggregates.get(curSaved));
            }
            savedAggregates.put(curOrder, curSavedAggregates);
            setNext.put(curOrder, curOrderNext);
            if (curOrder == 1) {
                for (IntSet i : curOrderNext) {
                    singleNextArray[i.getFirst()] = true;
                }
            }
            log.info("Time spent in order {}:  {} ms", curOrderFinal, System.currentTimeMillis() - startTime);
        }

        List<APLExplanationResult> results = new ArrayList<>();
        for (int curOrder: savedAggregates.keySet()) {
            Map<IntSet, double []> curOrderSavedAggregates = savedAggregates.get(curOrder);
            for (IntSet curSet : curOrderSavedAggregates.keySet()) {
                double[] aggregates = curOrderSavedAggregates.get(curSet);
                double[] metrics = new double[qualityMetrics.length];
                for (int i = 0; i < metrics.length; i++) {
                    metrics[i] = qualityMetrics[i].value(aggregates);
                }
                results.add(
                        new APLExplanationResult(qualityMetrics, curSet, aggregates, metrics)
                );
            }
        }
        return results;
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
            subPair = new IntSetAsArray(
                    curCandidate.getFirst(),
                    curCandidate.getSecond());
            if (o2Candidates.contains(subPair)) {
                subPair = new IntSetAsArray(
                        curCandidate.getSecond(),
                        curCandidate.getThird());
                if (o2Candidates.contains(subPair)) {
                    subPair = new IntSetAsArray(
                            curCandidate.getFirst(),
                            curCandidate.getThird());
                    return o2Candidates.contains(subPair);
                }
            }
        return false;
    }
}
