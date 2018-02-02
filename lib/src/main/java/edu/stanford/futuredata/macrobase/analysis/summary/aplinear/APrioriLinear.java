package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.*;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.util.MacrobaseInternalError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Class for handling the generic, algorithmic aspects of apriori explanation.
 * This class assumes that subgroups posses "aggregates" such as count and outlier_count
 * which can be combined additively. Then, we use APriori to find the subgroups which
 * are the most interesting as defined by "quality metrics" on these aggregates.
 */
public class APrioriLinear {
    private Logger log = LoggerFactory.getLogger("APLSummarizer");

    // **Parameters**
    private QualityMetric[] qualityMetrics;
    private double[] thresholds;

    // **Cached values**
    // Singleton viable sets for quick lookup
    private HashSet<Integer> singleNext;
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
            int cardinality,
            HashMap<Integer, Integer> columnDecoder
    ) {
        final int numAggregates = aggregateColumns.length;
        final int numRows = aggregateColumns[0].length;
        final int numColumns = attributes[0].length;
        // Number of threads in the parallelized sections of this function
        final int numThreads = Runtime.getRuntime().availableProcessors();

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

        // Shard the dataset by rows for the threads, but store it by column for fast processing
        final int[][][] byThreadAttributesTranspose =
                new int[numThreads][numColumns][(numRows + numThreads)/numThreads];
        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            final int startIndex = (numRows * threadNum) / numThreads;
            final int endIndex = (numRows * (threadNum + 1)) / numThreads;
            for(int i = 0; i < numColumns; i++)
                for(int j = startIndex; j < endIndex; j++) {
                    byThreadAttributesTranspose[threadNum][i][j - startIndex] = attributes[j][i];
                }
        }

        // Quality metrics are initialized with global aggregates to
        // allow them to determine the appropriate relative thresholds
        double[] globalAggregates = new double[numAggregates];
        for (int j = 0; j < numAggregates; j++) {
            globalAggregates[j] = 0;
            double[] curColumn = aggregateColumns[j];
            for (int i = 0; i < numRows; i++) {
                globalAggregates[j] += curColumn[i];
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
        for (int curOrder = 1; curOrder <= 3; curOrder++) {
            long startTime = System.currentTimeMillis();
            final int curOrderFinal = curOrder;
            // For order 3, pre-calculate all possible candidate sets from "next" sets of
            // previous orders. We will focus on aggregating results for these sets.
            final FastFixedHashSet precalculatedCandidates;
            if (curOrder == 3) {
                precalculatedCandidates = precalculateCandidates(columnDecoder, setNext.get(2),
                        singleNext, useIntSetAsArray);
            } else {
                precalculatedCandidates = null;
            }
            // Initialize per-thread hashmaps.
            final ArrayList<FastFixedHashTable> threadSetAggregates = new ArrayList<>(numThreads);
            int hashTableSize;
            if (curOrder == 1)
                hashTableSize = cardinality * 10;
            else if (curOrder == 2) {
                try {
                    hashTableSize = Math.multiplyExact(cardinality, cardinality);
                    hashTableSize = Math.multiplyExact(10, hashTableSize);
                } catch (ArithmeticException e) {
                    hashTableSize = Integer.MAX_VALUE;
                }
            }
            else
                hashTableSize = precalculatedCandidates.getCapacity();
            for (int i = 0; i < numThreads; i++) {
                threadSetAggregates.add(new FastFixedHashTable(hashTableSize, numAggregates, useIntSetAsArray));
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
                    if (curOrderFinal == 1) {
                        for (int colNum = 0; colNum < numColumns; colNum++) {
                            int[] curColumnAttributes = byThreadAttributesTranspose[curThreadNum][colNum];
                            for (int rowNum = startIndex; rowNum < endIndex; rowNum++) {
                                // Require that all order-one candidates have minimum support.
                                if (curColumnAttributes[rowNum - startIndex] == AttributeEncoder.noSupport)
                                    continue;
                                // Cascade to arrays if necessary, but otherwise pack attributes into longs.
                                if (useIntSetAsArray) {
                                    IntSet curCandidate = new IntSetAsArray(curColumnAttributes[rowNum - startIndex]);
                                    double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
                                        if (candidateVal == null) {
                                            thisThreadSetAggregates.put(curCandidate,
                                                    Arrays.copyOf(aRows[rowNum], numAggregates));
                                        } else {
                                            for (int a = 0; a < numAggregates; a++) {
                                                candidateVal[a] += aRows[rowNum][a];
                                            }
                                        }
                                } else {
                                    long curCandidate = curColumnAttributes[rowNum - startIndex];
                                    double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
                                    if (candidateVal == null) {
                                        thisThreadSetAggregates.put(curCandidate,
                                                Arrays.copyOf(aRows[rowNum], numAggregates));
                                    } else {
                                        for (int a = 0; a < numAggregates; a++) {
                                            candidateVal[a] += aRows[rowNum][a];
                                        }
                                    }
                                }
                            }
                        }
                    } else if (curOrderFinal == 2) {
                        for (int colNumOne = 0; colNumOne < numColumns; colNumOne++) {
                            int[] curColumnOneAttributes = byThreadAttributesTranspose[curThreadNum][colNumOne];
                            for (int colNumTwo = colNumOne + 1; colNumTwo < numColumns; colNumTwo++) {
                                int[] curColumnTwoAttributes = byThreadAttributesTranspose[curThreadNum][colNumTwo];
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
                                        IntSet curCandidate = new IntSetAsArray(curColumnOneAttributes[rowNumInCol],
                                                curColumnTwoAttributes[rowNumInCol]);
                                            double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
                                            if (candidateVal == null) {
                                                thisThreadSetAggregates.put(curCandidate,
                                                        Arrays.copyOf(aRows[rowNum], numAggregates));
                                            } else {
                                                for (int a = 0; a < numAggregates; a++) {
                                                    candidateVal[a] += aRows[rowNum][a];
                                                }
                                            }
                                    } else {
                                        long curCandidate = IntSetAsLong.twoIntToLong(curColumnOneAttributes[rowNumInCol],
                                                curColumnTwoAttributes[rowNumInCol]);
                                        double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
                                        if (candidateVal == null) {
                                            thisThreadSetAggregates.put(curCandidate,
                                                    Arrays.copyOf(aRows[rowNum], numAggregates));
                                        } else {
                                            for (int a = 0; a < numAggregates; a++) {
                                                candidateVal[a] += aRows[rowNum][a];
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else if (curOrderFinal == 3) {
                        for (int colNumOne = 0; colNumOne < numColumns; colNumOne++) {
                            int[] curColumnOneAttributes = byThreadAttributesTranspose[curThreadNum][colNumOne % numColumns];
                            for (int colNumTwo = colNumOne + 1; colNumTwo < numColumns; colNumTwo++) {
                                int[] curColumnTwoAttributes = byThreadAttributesTranspose[curThreadNum][colNumTwo % numColumns];
                                for (int colnumThree = colNumTwo + 1; colnumThree < numColumns; colnumThree++) {
                                    int[] curColumnThreeAttributes = byThreadAttributesTranspose[curThreadNum][colnumThree % numColumns];
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
                                            IntSet curCandidate = new IntSetAsArray(
                                                    curColumnOneAttributes[rowNumInCol],
                                                    curColumnTwoAttributes[rowNumInCol],
                                                    curColumnThreeAttributes[rowNumInCol]);
                                                // Require that all order-two subsets of a candidate have minimum support.
                                                if (!precalculatedCandidates.contains(curCandidate)) {
                                                    continue;
                                                }
                                                double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
                                                if (candidateVal == null) {
                                                    thisThreadSetAggregates.put(curCandidate,
                                                            Arrays.copyOf(aRows[rowNum], numAggregates));
                                                } else {
                                                    for (int a = 0; a < numAggregates; a++) {
                                                        candidateVal[a] += aRows[rowNum][a];
                                                    }
                                                }
                                        } else {
                                            long curCandidate = IntSetAsLong.threeIntToLong(
                                                    curColumnOneAttributes[rowNumInCol],
                                                    curColumnTwoAttributes[rowNumInCol],
                                                    curColumnThreeAttributes[rowNumInCol]);
                                            // Require that all order-two subsets of a candidate have minimum support.
                                            if (!precalculatedCandidates.contains(curCandidate)) {
                                                continue;
                                            }
                                            double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
                                            if (candidateVal == null) {
                                                thisThreadSetAggregates.put(curCandidate,
                                                        Arrays.copyOf(aRows[rowNum], numAggregates));
                                            } else {
                                                for (int a = 0; a < numAggregates; a++) {
                                                    candidateVal[a] += aRows[rowNum][a];
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        throw new MacrobaseInternalError("High Order not supported");
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
                                candidateVal[a] += curCandidateValue[a];
                            }
                        }
                    }
                } else {
                    for (long curCandidateKeyLong : set.keySetLong()) {
                        IntSetAsLong curCandidateKeyIntSetAsLong = new IntSetAsLong(curCandidateKeyLong);
                        IntSet curCandidateKey = new IntSetAsArray(curCandidateKeyIntSetAsLong);
                        double[] curCandidateValue = set.get(curCandidateKeyLong);
                        double[] candidateVal = setAggregates.get(curCandidateKey);
                        if (candidateVal == null) {
                            setAggregates.put(curCandidateKey, Arrays.copyOf(curCandidateValue, numAggregates));
                        } else {
                            for (int a = 0; a < numAggregates; a++) {
                                candidateVal[a] += curCandidateValue[a];
                            }
                        }
                    }
                }
            }

            // Prune all the collected aggregates
            HashSet<IntSet> curOrderNext = new HashSet<>();
            HashSet<IntSet> curOrderSaved = new HashSet<>();
            for (IntSet curCandidate: setAggregates.keySet()) {
                if (curOrder == 1 && curCandidate.getFirst() == AttributeEncoder.noSupport) {
                    continue;
                }
                double[] curAggregates = setAggregates.get(curCandidate);
                boolean canPassThreshold = true;
                boolean isPastThreshold = true;
                for (int i = 0; i < qualityMetrics.length; i++) {
                    QualityMetric q = qualityMetrics[i];
                    double t = thresholds[i];
                    canPassThreshold &= q.maxSubgroupValue(curAggregates) >= t;
                    isPastThreshold &= q.value(curAggregates) >= t;
                }
                if (canPassThreshold) {
                    // if a set is already past the threshold on all metrics,
                    // save it and no need for further exploration
                    if (isPastThreshold) {
                        curOrderSaved.add(curCandidate);
                    }
                    else {
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
                singleNext = new HashSet<>(curOrderNext.size());
                singleNextArray = new boolean[cardinality];
                for (IntSet i : curOrderNext) {
                    singleNext.add(i.getFirst());
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
     * Precalculate all possible candidates of order 3 to speed up candidate
     * generation.
     * @param o2Candidates All candidates of order 2 with minimum support.
     * @param singleCandidates All singleton candidates with minimum support.
     * @return Possible candidates of order 3.
     */
    private FastFixedHashSet precalculateCandidates(HashMap<Integer, Integer> columnDecoder,
                                                    HashSet<IntSet> o2Candidates,
                                                    HashSet<Integer> singleCandidates,
                                                    boolean useIntSetAsArray) {
        HashSet<IntSet> candidates = new HashSet<>(o2Candidates.size() * singleCandidates.size() / 2);
        for (IntSet pCandidate : o2Candidates) {
            for (Integer sCandidate : singleCandidates) {
                if (!pCandidate.contains(sCandidate)) {
                    IntSet nCandidate = new IntSetAsArray(
                            pCandidate.getFirst(),
                            pCandidate.getSecond(),
                            sCandidate,
                            columnDecoder);
                    candidates.add(nCandidate);
                }
            }
        }
        List<IntSet> finalCandidatesList = new ArrayList<>();
        IntSet subPair;
        for (IntSet curCandidate : candidates) {
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
                    if (o2Candidates.contains(subPair)) {
                        finalCandidatesList.add(curCandidate);
                    }
                }
            }
        }
        FastFixedHashSet finalCandidates = new FastFixedHashSet(finalCandidatesList.size() * 10, useIntSetAsArray);
        for (IntSet candidate: finalCandidatesList) {
            if (useIntSetAsArray) {
                finalCandidates.add(candidate);
            } else {
                finalCandidates.add(candidate.toLong());
            }
        }
        return finalCandidates;
    }
}
