package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.*;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.Random;
import java.util.BitSet;

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
    // Singleton viable sets for quick lookup
    private boolean[] singleNextArray;
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
            HashMap<Integer, BitSet>[][] bitmap,
            ArrayList<Integer>[] outlierList,
            boolean[] isBitmapEncoded
    ) {
        final int numAggregates = aggregateColumns.length;
        final int numRows = aggregateColumns[0].length;
        final int numColumns = attributes[0].length;

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
        System.out.println("numThreads: " + numThreads);
        // Shard the dataset by rows for the threads, but store it by column for fast processing
        final int[][][] byThreadAttributesTranspose =
                new int[numThreads][numColumns][(numRows + numThreads)/numThreads];
        final HashMap<Integer, BitSet>[][][] byThreadBitmap = new HashMap[numThreads][numColumns][2];
        for (int i = 0; i < numThreads; i++)
            for (int j = 0; j < numColumns; j++)
                for (int k = 0; k < 2; k++)
                    byThreadBitmap[i][j][k] = new HashMap<>();
        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            final int startIndex = (numRows * threadNum) / numThreads;
            final int endIndex = (numRows * (threadNum + 1)) / numThreads;
            for(int i = 0; i < numColumns; i++) {
                if (!isBitmapEncoded[i]) {
                    for (int j = startIndex; j < endIndex; j++) {
                        byThreadAttributesTranspose[threadNum][i][j - startIndex] = attributes[j][i];
                    }
                } else {
                    for (int j = 0; j < 2; j++) {
                        for (HashMap.Entry<Integer, BitSet> entry : bitmap[i][j].entrySet()) {
                            BitSet b = new BitSet();
                            b.set(startIndex, endIndex);
                            b.and(entry.getValue());
                            if (b.cardinality() > 0) {
                                byThreadBitmap[threadNum][i][j].put(entry.getKey(), b);
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
                            if (isBitmapEncoded[colNum]) {
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
                                    updateAggregates(thisThreadSetAggregates, curCandidate, new double[]{outlierCount, outlierCount + inlierCount}, numAggregates);
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
                                    updateAggregates(thisThreadSetAggregates, curCandidate, aRows[rowNum], numAggregates);
                                }
                            }
                        }
                    } else if (curOrderFinal == 2) {
                        for (int colNumOne = 0; colNumOne < numColumns; colNumOne++) {
                            int[] curColumnOneAttributes = byThreadAttributesTranspose[curThreadNum][colNumOne];
                            for (int colNumTwo = colNumOne + 1; colNumTwo < numColumns; colNumTwo++) {
                                int[] curColumnTwoAttributes = byThreadAttributesTranspose[curThreadNum][colNumTwo];

                                if (isBitmapEncoded[colNumOne] && isBitmapEncoded[colNumTwo]) {
                                    // Bitmap-Bitmap
                                    allTwoBitmap(thisThreadSetAggregates, outlierList, byThreadBitmap[curThreadNum],
                                            colNumOne, colNumTwo, useIntSetAsArray, curCandidate, numAggregates);
                                } else if (isBitmapEncoded[colNumOne] && !isBitmapEncoded[colNumTwo]) {
                                    // Bitmap-Normal
                                    allOneBitmapOneNormal(thisThreadSetAggregates, outlierList[colNumOne], byThreadBitmap[curThreadNum][colNumOne],
                                            curColumnTwoAttributes, startIndex, useIntSetAsArray, curCandidate, numAggregates);
                                } else if (!isBitmapEncoded[colNumOne] && isBitmapEncoded[colNumTwo]) {
                                    // Normal-Bitmap
                                    allOneBitmapOneNormal(thisThreadSetAggregates, outlierList[colNumTwo], byThreadBitmap[curThreadNum][colNumTwo],
                                            curColumnOneAttributes, startIndex, useIntSetAsArray, curCandidate, numAggregates);
                                } else {
                                    // Normal-Normal
                                    allTwoNormal(thisThreadSetAggregates, curColumnOneAttributes, curColumnTwoAttributes,
                                            startIndex, endIndex, useIntSetAsArray, curCandidate, aRows, numAggregates);
                                }
                            }
                        }
                    } else if (curOrderFinal == 3) {
                        // order-3 case
                        for (int colNumOne = 0; colNumOne < numColumns; colNumOne++) {
                            int[] curColumnOneAttributes = byThreadAttributesTranspose[curThreadNum][colNumOne % numColumns];
                            for (int colNumTwo = colNumOne + 1; colNumTwo < numColumns; colNumTwo++) {
                                int[] curColumnTwoAttributes = byThreadAttributesTranspose[curThreadNum][colNumTwo % numColumns];
                                for (int colNumThree = colNumTwo + 1; colNumThree < numColumns; colNumThree++) {
                                    int[] curColumnThreeAttributes = byThreadAttributesTranspose[curThreadNum][colNumThree % numColumns];

                                    if (isBitmapEncoded[colNumOne] && isBitmapEncoded[colNumTwo] && isBitmapEncoded[colNumThree]) {
                                        // all 3 cols are bitmaps
                                        allThreeBitmap(thisThreadSetAggregates, outlierList, byThreadBitmap[curThreadNum],
                                                colNumOne, colNumTwo, colNumThree, useIntSetAsArray, curCandidate, numAggregates);

                                    } else if (isBitmapEncoded[colNumOne] && isBitmapEncoded[colNumTwo] && !isBitmapEncoded[colNumThree]) {
                                        // one and two are bitmaps, 3 is normal
                                        allTwoBitmapsOneNormal(thisThreadSetAggregates, outlierList, byThreadBitmap[curThreadNum], colNumOne, colNumTwo,
                                                curColumnThreeAttributes, startIndex, useIntSetAsArray, curCandidate, numAggregates);

                                    } else if (isBitmapEncoded[colNumOne] && !isBitmapEncoded[colNumTwo] && isBitmapEncoded[colNumThree]) {
                                        // one and three are bitmaps, 2 is normal
                                        allTwoBitmapsOneNormal(thisThreadSetAggregates, outlierList, byThreadBitmap[curThreadNum], colNumOne, colNumThree,
                                                curColumnTwoAttributes, startIndex, useIntSetAsArray, curCandidate, numAggregates);

                                    } else if (!isBitmapEncoded[colNumOne] && isBitmapEncoded[colNumTwo] && isBitmapEncoded[colNumThree]) {
                                        // two and three are bitmaps, 1 is normal
                                        allTwoBitmapsOneNormal(thisThreadSetAggregates, outlierList, byThreadBitmap[curThreadNum], colNumTwo, colNumThree,
                                                curColumnOneAttributes, startIndex, useIntSetAsArray, curCandidate, numAggregates);

                                    } else if (isBitmapEncoded[colNumOne] && !isBitmapEncoded[colNumTwo] && !isBitmapEncoded[colNumThree]) {
                                        // one is a bitmap, 2 and 3 are normal
                                        allOneBitmapTwoNormal(thisThreadSetAggregates, outlierList[colNumOne], byThreadBitmap[curThreadNum][colNumOne],
                                                curColumnTwoAttributes, curColumnThreeAttributes, startIndex, useIntSetAsArray, curCandidate, numAggregates);

                                    } else if (!isBitmapEncoded[colNumOne] && isBitmapEncoded[colNumTwo] && !isBitmapEncoded[colNumThree]) {
                                        // two is a bitmap, 1 and 3 are normal
                                        allOneBitmapTwoNormal(thisThreadSetAggregates, outlierList[colNumTwo], byThreadBitmap[curThreadNum][colNumTwo],
                                                curColumnOneAttributes, curColumnThreeAttributes, startIndex, useIntSetAsArray, curCandidate, numAggregates);

                                    } else if (!isBitmapEncoded[colNumOne] && !isBitmapEncoded[colNumTwo] && isBitmapEncoded[colNumThree]) {
                                        // three is a bitmap, 1 and 2 are normal
                                        allOneBitmapTwoNormal(thisThreadSetAggregates, outlierList[colNumThree], byThreadBitmap[curThreadNum][colNumThree],
                                                curColumnOneAttributes, curColumnTwoAttributes, startIndex, useIntSetAsArray, curCandidate, numAggregates);
                                    } else {
                                        // all three are normal
                                        allThreeNormal(thisThreadSetAggregates, curColumnOneAttributes, curColumnTwoAttributes, curColumnThreeAttributes,
                                                startIndex, endIndex, useIntSetAsArray, curCandidate, aRows, numAggregates);
                                    }
                                }
                            }
                        }
                    }
                    log.debug("Time spent in Thread {} in order {}:  {} ms",
                            curThreadNum, curOrderFinal, System.currentTimeMillis() - startTime);
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
                singleNextArray = new boolean[cardinality];
                for (IntSet i : curOrderNext) {
                    singleNextArray[i.getFirst()] = true;
                }
            }
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

    private void updateAggregates(FastFixedHashTable thisThreadSetAggregates, IntSet curCandidate, double[] val, int numAggregates) {
        double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
        if (candidateVal == null) {
            thisThreadSetAggregates.put(curCandidate,
                    Arrays.copyOf(val, numAggregates));
        } else {
            for (int a = 0; a < numAggregates; a++) {
                candidateVal[a] += val[a];
            }
        }
    }

    /*********************** All Order-2 helper methods ***********************/

    // One Bitmap, One Normal
    private void allOneBitmapOneNormal(FastFixedHashTable thisThreadSetAggregates,
                                       ArrayList<Integer> outlierColList,
                                       HashMap<Integer, BitSet>[] byThreadColumnBitmap,
                                       int[] curColumnTwoAttributes, int startIndex,
                                       boolean useIntSetAsArray, IntSet curCandidate, int numAggregates) {
        for (Integer curCandidateOne : outlierColList) {
            if (curCandidateOne == AttributeEncoder.noSupport || !singleNextArray[curCandidateOne])
                continue;
            if (byThreadColumnBitmap[1].containsKey(curCandidateOne)) {
                BitSet outlierBitmap = byThreadColumnBitmap[1].get(curCandidateOne);
                // pass in Array of [1, 1] for [outlier_count_col, total_count_col]
                oneBitmapOneNormal(thisThreadSetAggregates, outlierBitmap, curCandidateOne,
                        curColumnTwoAttributes, startIndex, useIntSetAsArray, curCandidate, new double[]{1, 1}, numAggregates);
            }
            if (byThreadColumnBitmap[0].containsKey(curCandidateOne)) {
                BitSet inlierBitmap = byThreadColumnBitmap[0].get(curCandidateOne);
                // pass in Array of [0, 1] for [outlier_count_col, total_count_col] (since this is for inliers)
                oneBitmapOneNormal(thisThreadSetAggregates, inlierBitmap, curCandidateOne,
                        curColumnTwoAttributes, startIndex, useIntSetAsArray, curCandidate, new double[]{0, 1}, numAggregates);
            }
        }
    }

    private void oneBitmapOneNormal(FastFixedHashTable thisThreadSetAggregates,
                                    BitSet bitmap, Integer curCandidateOne,
                                    int[] curColumnTwoAttributes, int startIndex,
                                    boolean useIntSetAsArray, IntSet curCandidate,
                                    double[] val, int numAggregates) {
        for (int rowNum = bitmap.nextSetBit(0); rowNum >= 0; rowNum = bitmap.nextSetBit(rowNum + 1)) {
            int rowNumInCol = rowNum - startIndex;
            if (curColumnTwoAttributes[rowNumInCol] == AttributeEncoder.noSupport || !singleNextArray[curColumnTwoAttributes[rowNumInCol]])
                continue;
            // Cascade to arrays if necessary, but otherwise pack attributes into longs.
            if (useIntSetAsArray) {
                curCandidate = new IntSetAsArray(curCandidateOne, curColumnTwoAttributes[rowNumInCol]);
            } else {
                ((IntSetAsLong) curCandidate).value = IntSetAsLong.twoIntToLong(curCandidateOne, curColumnTwoAttributes[rowNumInCol]);
            }
            updateAggregates(thisThreadSetAggregates, curCandidate, val, numAggregates);
        }
    }

    // Two Normal columns
    private void allTwoNormal(FastFixedHashTable thisThreadSetAggregates,
                              int[] curColumnOneAttributes, int[] curColumnTwoAttributes,
                              int startIndex, int endIndex,
                              boolean useIntSetAsArray, IntSet curCandidate,
                              double[][] aRows, int numAggregates) {
        for (int rowNum = startIndex; rowNum < endIndex; rowNum++) {
            int rowNumInCol = rowNum - startIndex;
            // Only examine a pair if both its members have minimum support.
            if (curColumnOneAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || curColumnTwoAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || !singleNextArray[curColumnOneAttributes[rowNumInCol]]
                    || !singleNextArray[curColumnTwoAttributes[rowNumInCol]])
                continue;
            // Cascade to arrays if necessary, but otherwise pack attributes into longs.
            if (useIntSetAsArray) {
                curCandidate = new IntSetAsArray(curColumnOneAttributes[rowNumInCol],
                        curColumnTwoAttributes[rowNumInCol]);
            } else {
                ((IntSetAsLong) curCandidate).value = IntSetAsLong.twoIntToLong(curColumnOneAttributes[rowNumInCol],
                        curColumnTwoAttributes[rowNumInCol]);
            }
            updateAggregates(thisThreadSetAggregates, curCandidate, aRows[rowNum], numAggregates);
        }
    }

    // Two bitmap columns
    private void allTwoBitmap(FastFixedHashTable thisThreadSetAggregates,
                              ArrayList<Integer>[] outlierList,
                              HashMap<Integer, BitSet>[][] byThreadBitmap,
                              int colNumOne, int colNumTwo,
                              boolean useIntSetAsArray, IntSet curCandidate, int numAggregates) {
        for (Integer curCandidateOne : outlierList[colNumOne]) {
            if (curCandidateOne == AttributeEncoder.noSupport || !singleNextArray[curCandidateOne])
                continue;
            for (Integer curCandidateTwo : outlierList[colNumTwo]) {
                if (curCandidateTwo == AttributeEncoder.noSupport || !singleNextArray[curCandidateTwo])
                    continue;
                // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                if (useIntSetAsArray) {
                    curCandidate = new IntSetAsArray(curCandidateOne, curCandidateTwo);
                } else {
                    ((IntSetAsLong) curCandidate).value = IntSetAsLong.twoIntToLong(curCandidateOne, curCandidateTwo);
                }
                int outlierCount = 0, inlierCount = 0;
                if (byThreadBitmap[colNumOne][1].containsKey(curCandidateOne) &&
                        byThreadBitmap[colNumTwo][1].containsKey(curCandidateTwo)) {
                    BitSet b = (BitSet)byThreadBitmap[colNumOne][1].get(curCandidateOne).clone();
                    b.and(byThreadBitmap[colNumTwo][1].get(curCandidateTwo));
                    outlierCount = b.cardinality();
                }
                if (byThreadBitmap[colNumOne][0].containsKey(curCandidateOne) &&
                        byThreadBitmap[colNumTwo][0].containsKey(curCandidateTwo)) {
                    BitSet b = (BitSet)byThreadBitmap[colNumOne][0].get(curCandidateOne).clone();
                    b.and(byThreadBitmap[colNumTwo][0].get(curCandidateTwo));
                    inlierCount = b.cardinality();
                }
                updateAggregates(thisThreadSetAggregates, curCandidate, new double[]{outlierCount, outlierCount + inlierCount}, numAggregates);
            }
        }
    }

    /*********************** All Order-3 helper methods ***********************/

    // One Bitmap, Two Normal
    private void allOneBitmapTwoNormal(FastFixedHashTable thisThreadSetAggregates,
                                       ArrayList<Integer> outlierColList,
                                       HashMap<Integer, BitSet>[] byThreadColumnBitmap,
                                       int[] curColumnTwoAttributes, int[] curColumnThreeAttributes, int startIndex,
                                       boolean useIntSetAsArray, IntSet curCandidate, int numAggregates) {
        for (Integer curCandidateOne : outlierColList) {
            if (curCandidateOne == AttributeEncoder.noSupport || !singleNextArray[curCandidateOne])
                continue;
            if (byThreadColumnBitmap[1].containsKey(curCandidateOne)) {
                BitSet outlierBitmap = byThreadColumnBitmap[1].get(curCandidateOne);
                // pass in Array of [1, 1] for [outlier_count_col, total_count_col]
                oneBitmapTwoNormal(thisThreadSetAggregates, outlierBitmap, curCandidateOne,
                        curColumnTwoAttributes, curColumnThreeAttributes, startIndex, useIntSetAsArray, curCandidate, new double[]{1, 1}, numAggregates);
            }
            if (byThreadColumnBitmap[0].containsKey(curCandidateOne)) {
                BitSet inlierBitmap = byThreadColumnBitmap[0].get(curCandidateOne);
                // pass in Array of [0, 1] for [outlier_count_col, total_count_col] (since this is for the inliers)
                oneBitmapTwoNormal(thisThreadSetAggregates, inlierBitmap, curCandidateOne,
                        curColumnTwoAttributes, curColumnThreeAttributes, startIndex, useIntSetAsArray, curCandidate, new double[]{0, 1}, numAggregates);
            }
        }
    }

    private void oneBitmapTwoNormal(FastFixedHashTable thisThreadSetAggregates,
                                    BitSet bitmap, Integer curCandidateOne,
                                    int[] curColumnTwoAttributes, int[] curColumnThreeAttributes, int startIndex,
                                    boolean useIntSetAsArray, IntSet curCandidate,
                                    double[] val, int numAggregates) {
        for (int rowNum = bitmap.nextSetBit(0); rowNum >= 0; rowNum = bitmap.nextSetBit(rowNum + 1)) {
            int rowNumInCol = rowNum - startIndex;
            if (curColumnTwoAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || curColumnThreeAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || !singleNextArray[curColumnTwoAttributes[rowNumInCol]]
                    || !singleNextArray[curColumnThreeAttributes[rowNumInCol]])
                continue;
            // Cascade to arrays if necessary, but otherwise pack attributes into longs.
            if (useIntSetAsArray) {
                curCandidate = new IntSetAsArray(
                        curCandidateOne,
                        curColumnTwoAttributes[rowNumInCol],
                        curColumnThreeAttributes[rowNumInCol]);
            } else {
                ((IntSetAsLong) curCandidate).value = IntSetAsLong.threeIntToLong(
                        curCandidateOne,
                        curColumnTwoAttributes[rowNumInCol],
                        curColumnThreeAttributes[rowNumInCol]);
            }
            updateAggregates(thisThreadSetAggregates, curCandidate, val, numAggregates);
        }
    }

    // Two Bitmaps, One Normal
    private void allTwoBitmapsOneNormal(FastFixedHashTable thisThreadSetAggregates,
                                        ArrayList<Integer>[] outlierList,
                                        HashMap<Integer, BitSet>[][] byThreadBitmap,
                                        int colNumOne, int colNumTwo,
                                        int[] curColumnThreeAttributes, int startIndex,
                                        boolean useIntSetAsArray, IntSet curCandidate, int numAggregates) {
        for (Integer curCandidateOne : outlierList[colNumOne]) {
            if (curCandidateOne == AttributeEncoder.noSupport || !singleNextArray[curCandidateOne])
                continue;
            for (Integer curCandidateTwo : outlierList[colNumTwo]) {
                if (curCandidateTwo == AttributeEncoder.noSupport || !singleNextArray[curCandidateTwo])
                    continue;

                if (byThreadBitmap[colNumOne][1].containsKey(curCandidateOne) && byThreadBitmap[colNumTwo][1].containsKey(curCandidateTwo)) {
                    BitSet outlierBitmap = (BitSet)byThreadBitmap[colNumOne][1].get(curCandidateOne).clone();
                    outlierBitmap.and(byThreadBitmap[colNumTwo][1].get(curCandidateTwo));
                    twoBitmapsOneNormal(thisThreadSetAggregates, outlierBitmap, curCandidateOne, curCandidateTwo,
                            curColumnThreeAttributes, startIndex, useIntSetAsArray, curCandidate, new double[]{1, 1}, numAggregates);
                }
                if (byThreadBitmap[colNumOne][0].containsKey(curCandidateOne) && byThreadBitmap[colNumTwo][0].containsKey(curCandidateTwo)) {
                    BitSet inlierBitmap = (BitSet)byThreadBitmap[colNumOne][0].get(curCandidateOne).clone();
                    inlierBitmap.and(byThreadBitmap[colNumTwo][0].get(curCandidateTwo));
                    twoBitmapsOneNormal(thisThreadSetAggregates, inlierBitmap, curCandidateOne, curCandidateTwo,
                            curColumnThreeAttributes, startIndex, useIntSetAsArray, curCandidate, new double[]{0, 1}, numAggregates);
                }
            }
        }
    }

    private void twoBitmapsOneNormal(FastFixedHashTable thisThreadSetAggregates,
                                     BitSet bitmap, Integer curCandidateOne, Integer curCandidateTwo,
                                     int[] curColumnThreeAttributes, int startIndex,
                                     boolean useIntSetAsArray, IntSet curCandidate,
                                     double[] val, int numAggregates) {
        for (int rowNum = bitmap.nextSetBit(0); rowNum >= 0; rowNum = bitmap.nextSetBit(rowNum + 1)) {
            int rowNumInCol = rowNum - startIndex;
            if (curColumnThreeAttributes[rowNumInCol] == AttributeEncoder.noSupport || !singleNextArray[curColumnThreeAttributes[rowNumInCol]])
                continue;
            // Cascade to arrays if necessary, but otherwise pack attributes into longs.
            if (useIntSetAsArray) {
                curCandidate = new IntSetAsArray(
                        curCandidateOne,
                        curCandidateTwo,
                        curColumnThreeAttributes[rowNumInCol]);
            } else {
                ((IntSetAsLong) curCandidate).value = IntSetAsLong.threeIntToLong(
                        curCandidateOne,
                        curCandidateTwo,
                        curColumnThreeAttributes[rowNumInCol]);
            }
            updateAggregates(thisThreadSetAggregates, curCandidate, val, numAggregates);
        }
    }

    // All Three Normal or All Three Bitmap
    private void allThreeNormal(FastFixedHashTable thisThreadSetAggregates,
                                int[] curColumnOneAttributes, int[] curColumnTwoAttributes, int[] curColumnThreeAttributes,
                                int startIndex, int endIndex,
                                boolean useIntSetAsArray, IntSet curCandidate,
                                double[][] aRows, int numAggregates) {
        for (int rowNum = startIndex; rowNum < endIndex; rowNum++) {
            int rowNumInCol = rowNum - startIndex;
            // Only construct a triple if all its singleton members have minimum support.
            if (curColumnOneAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || curColumnTwoAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || curColumnThreeAttributes[rowNumInCol] == AttributeEncoder.noSupport
                    || !singleNextArray[curColumnThreeAttributes[rowNumInCol]]
                    || !singleNextArray[curColumnOneAttributes[rowNumInCol]]
                    || !singleNextArray[curColumnTwoAttributes[rowNumInCol]])
                continue;
            // Cascade to arrays if necessary, but otherwise pack attributes into longs.
            if (useIntSetAsArray) {
                curCandidate = new IntSetAsArray(
                        curColumnOneAttributes[rowNumInCol],
                        curColumnTwoAttributes[rowNumInCol],
                        curColumnThreeAttributes[rowNumInCol]);
            } else {
                ((IntSetAsLong) curCandidate).value = IntSetAsLong.threeIntToLong(
                        curColumnOneAttributes[rowNumInCol],
                        curColumnTwoAttributes[rowNumInCol],
                        curColumnThreeAttributes[rowNumInCol]);
            }
            updateAggregates(thisThreadSetAggregates, curCandidate, aRows[rowNum], numAggregates);
        }
    }

    private void allThreeBitmap(FastFixedHashTable thisThreadSetAggregates,
                                ArrayList<Integer>[] outlierList,
                                HashMap<Integer, BitSet>[][] byThreadBitmap,
                                int colNumOne, int colNumTwo, int colNumThree,
                                boolean useIntSetAsArray, IntSet curCandidate, int numAggregates) {

        for (Integer curCandidateOne : outlierList[colNumOne]) {
            if (curCandidateOne == AttributeEncoder.noSupport || !singleNextArray[curCandidateOne])
                continue;
            for (Integer curCandidateTwo : outlierList[colNumTwo]) {
                if (curCandidateTwo == AttributeEncoder.noSupport || !singleNextArray[curCandidateTwo])
                    continue;
                for (Integer curCandidateThree : outlierList[colNumThree]) {
                    if (curCandidateThree == AttributeEncoder.noSupport || !singleNextArray[curCandidateThree])
                        continue;
                    // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                    if (useIntSetAsArray) {
                        curCandidate = new IntSetAsArray(
                                curCandidateOne,
                                curCandidateTwo,
                                curCandidateThree);
                    } else {
                        ((IntSetAsLong) curCandidate).value = IntSetAsLong.threeIntToLong(
                                curCandidateOne,
                                curCandidateTwo,
                                curCandidateThree);
                    }
                    int outlierCount = 0, inlierCount = 0;
                    // index 1 is the outlier bitmap
                    if (byThreadBitmap[colNumOne][1].containsKey(curCandidateOne) &&
                            byThreadBitmap[colNumTwo][1].containsKey(curCandidateTwo) &&
                            byThreadBitmap[colNumThree][1].containsKey(curCandidateThree)) {
                                BitSet b = (BitSet)byThreadBitmap[colNumOne][1].get(curCandidateOne).clone();
                                b.and(byThreadBitmap[colNumTwo][1].get(curCandidateTwo));
                                b.and(byThreadBitmap[colNumThree][1].get(curCandidateThree));
                                outlierCount = b.cardinality();
                    }
                    if (byThreadBitmap[colNumOne][0].containsKey(curCandidateOne) &&
                            byThreadBitmap[colNumTwo][0].containsKey(curCandidateTwo) &&
                            byThreadBitmap[colNumThree][0].containsKey(curCandidateThree)) {
                        BitSet b = (BitSet)byThreadBitmap[colNumOne][0].get(curCandidateOne).clone();
                        b.and(byThreadBitmap[colNumTwo][0].get(curCandidateTwo));
                        b.and(byThreadBitmap[colNumThree][0].get(curCandidateThree));
                        inlierCount = b.cardinality();
                    }

                    updateAggregates(thisThreadSetAggregates, curCandidate, new double[]{outlierCount, outlierCount + inlierCount}, numAggregates);
                }
            }
        }
    }
}
