package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.apriori.IntSet;
import edu.stanford.futuredata.macrobase.util.MacrobaseInternalError;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.IntSetAsLong.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Class for handling the generic, algorithmic aspects of apriori explanation.
 * This class assumes that subgroups posses "aggregates" such as count and outlier_count
 * which can be combined additively. Then, we use APriori to find the subgroups which
 * are the most interesting as defined by "quality metrics" on these aggregates.
 */
public class APrioriLinear {
    Logger log = LoggerFactory.getLogger("APLSummarizer");

    // **Parameters**
    private QualityMetric[] qualityMetrics;
    private double[] thresholds;

    // **Cached values**

    // Singleton viable sets for quick lookup
    private LongOpenHashSet singleNext;
    private boolean[] singleNextArray;
    // Sets that has high enough support but not high risk ratio, need to be explored
    private HashMap<Integer, LongOpenHashSet> setNext;
    // An array of pairs for quick lookup
    private boolean[] pairNextArray;
    // Aggregate values for all of the sets we saved
    private HashMap<Integer, Long2ObjectOpenHashMap<double []>> savedAggregates;

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
            int cardinality
    ) {
        final int numAggregates = aggregateColumns.length;
        final int numRows = aggregateColumns[0].length;
        // The smallest integer x such that 2**x > cardinality
        final long logCardinality = Math.round(Math.ceil(0.01 + Math.log(cardinality)/Math.log(2.0)));
        // Defined as 2 ** logCardinality
        final int ceilCardinality = Math.toIntExact(Math.round(Math.pow(2, logCardinality)));

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
            // Ceiling on attribute values that will be perfect-hashed instead of mapped.  Cannot be greater than
            // 32/curOrder or indices will not fit in an int.
            int perfectHashingThresholdExponent = 8;
            // Equal to 2**perfectHashingThresholdExponent
            int perfectHashingThreshold = Math.toIntExact(Math.round(Math.pow(2, perfectHashingThresholdExponent)));
            // The maximum size of the perfect hash array implied by perfectHashingThreshold
            int maxPerfectHashArraySize = Math.toIntExact(Math.round(Math.pow(perfectHashingThreshold, curOrder)));
            // The size of the perfect hash array implied by the cardinality of the input
            int cardPerfectHashArraySize = Math.toIntExact(Math.round(Math.pow(ceilCardinality, curOrder)));
            // The size of the perfect hash array, threadSetAggregatesArray, capped by maxPerfectHashArraySize
            int perfectHashArraySize = Math.min(maxPerfectHashArraySize, cardPerfectHashArraySize);
            // A mask used to quickly determine if a candidate should be perfect-hashed.
            long mask;
            if (maxPerfectHashArraySize < cardPerfectHashArraySize)
                mask = IntSetAsLong.checkLessThanMaskCreate(perfectHashingThreshold, logCardinality);
            else // In this case, the mask is unnecessary as everything is perfect-hashed.
                mask = 0;
            // Precalculate all possible candidate sets from "next" sets of
            // previous orders. We will focus on aggregating results for these
            // sets.
            final boolean[] precalculatedCandidates = precalculateCandidates(curOrder, logCardinality);
            // Run the critical path of the algorithm--candidate generation--in parallel.
            final int curOrderFinal = curOrder;
            final int numThreads = 1;//Runtime.getRuntime().availableProcessors();
            // The perfect hash-table.  This is not synchronized, so all values in it are approximate.
            final double [][] threadSetAggregatesArray = new double[perfectHashArraySize][numAggregates];
            // The per-thread hashmaps.  These store less-common values.
            final ArrayList<Long2ObjectOpenHashMap<double []>> threadSetAggregates = new ArrayList<>(numThreads);
            for (int i = 0; i < numThreads; i++) {
                threadSetAggregates.add(new Long2ObjectOpenHashMap<>());
            }
            final CountDownLatch doneSignal = new CountDownLatch(numThreads);
            // Shard the dataset by row into threads.
            for (int threadNum = 0; threadNum < numThreads; threadNum++) {
                final int startIndex = (numRows * threadNum) / numThreads;
                final int endIndex = (numRows * (threadNum + 1)) / numThreads;
                final Long2ObjectOpenHashMap<double []> thisThreadSetAggregates = threadSetAggregates.get(threadNum);
                // Do the critical path calculation in a lambda
                Runnable APrioriLinearRunnable = () -> {
                        for (int i = startIndex; i < endIndex; i++) {
                            int[] curRowAttributes = attributes[i];
                            // First, generate the candidates
                            ArrayList<Long> candidates = getCandidates(
                                    curOrderFinal,
                                    curRowAttributes,
                                    precalculatedCandidates,
                                    logCardinality
                            );
                            // Next, aggregate candidates.  This loop uses a hybrid system where common candidates are
                            // perfect-hashed for speed while less common ones are stored in a hash table.
                            for (long curCandidate: candidates) {
                                // If all components of a candidate are in the perfectHashingThreshold most common,
                                // store it in the perfect hash table.
                                if(IntSetAsLong.checkLessThanMask(curCandidate, mask)) {
                                     if (maxPerfectHashArraySize < cardPerfectHashArraySize) {
                                        curCandidate = IntSetAsLong.changePacking(curCandidate,
                                                        perfectHashingThresholdExponent, logCardinality);
                                    }
                                    for (int a = 0; a < numAggregates; a++) {
                                        threadSetAggregatesArray[(int) curCandidate][a] += aRows[i][a];
                                    }
                                // Otherwise, store it in a hashmap.
                                } else {
                                    double[] candidateVal = thisThreadSetAggregates.get(curCandidate);
                                    if (candidateVal == null) {
                                        thisThreadSetAggregates.put(curCandidate, Arrays.copyOf(aRows[i], numAggregates));
                                    } else {
                                        for (int a = 0; a < numAggregates; a++) {
                                            candidateVal[a] += aRows[i][a];
                                        }
                                    }
                                }
                            }
                        }
                        doneSignal.countDown();
                };
                // Run numThreads lambdas in separate threads
                Thread APrioriLinearThread = new Thread(APrioriLinearRunnable);
                APrioriLinearThread.start();
            }
            try {
                doneSignal.await();
            } catch (InterruptedException ex) {ex.printStackTrace();}

            // Collect the aggregates stored in the perfect hash table.
            Long2ObjectOpenHashMap<double []> setAggregates = new Long2ObjectOpenHashMap<>();
            for (int curCandidateKey = 0;  curCandidateKey < perfectHashArraySize; curCandidateKey++) {
                if (threadSetAggregatesArray[curCandidateKey][0] == 0 && threadSetAggregatesArray[curCandidateKey][1] == 0) continue;
                double[] curCandidateValue = threadSetAggregatesArray[curCandidateKey];
                int newCurCandidateKey = curCandidateKey;
                if (maxPerfectHashArraySize < cardPerfectHashArraySize) {
                    newCurCandidateKey = (int) IntSetAsLong.changePacking(curCandidateKey,
                            logCardinality, perfectHashingThresholdExponent);
                }
                double[] candidateVal = setAggregates.get(newCurCandidateKey);
                if (candidateVal == null) {
                    setAggregates.put(newCurCandidateKey, Arrays.copyOf(curCandidateValue, numAggregates));
                } else {
                    for (int a = 0; a < numAggregates; a++) {
                        candidateVal[a] += curCandidateValue[a];
                    }
                }
            }

            // Collect the aggregates stored in the per-thread hashmaps.
            for (Long2ObjectOpenHashMap<double []> set : threadSetAggregates) {
                for (long curCandidateKey : set.keySet()) {
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
            }

            // Prune all the collected aggregates
            LongOpenHashSet curOrderNext = new LongOpenHashSet();
            LongOpenHashSet curOrderSaved = new LongOpenHashSet();
            int pruned = 0;
            for (long curCandidate: setAggregates.keySet()) {
                if (curOrder == 1 && curCandidate == AttributeEncoder.noSupport) {
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
                } else {
                    pruned++;
                }
            }

            System.out.printf("Order: %d setAggregates: %d curOrderNext: %d\n", curOrder, setAggregates.keySet().size(), curOrderNext.size());

            Long2ObjectOpenHashMap<double []> curSavedAggregates = new Long2ObjectOpenHashMap<>(curOrderSaved.size());
            for (long curSaved : curOrderSaved) {
                curSavedAggregates.put(curSaved, setAggregates.get(curSaved));
            }
            savedAggregates.put(curOrder, curSavedAggregates);
            setNext.put(curOrder, curOrderNext);
            if (curOrder == 1) {
                singleNext = new LongOpenHashSet(curOrderNext.size());
                singleNextArray = new boolean[cardinality];
                for (long i : curOrderNext) {
                    singleNext.add(i);
                    // No need to hash because only one int in the long
                    singleNextArray[(int) i] = true;
                }
            } else if (curOrder == 2) {
                pairNextArray = new boolean[ceilCardinality * ceilCardinality];
                for (long i : curOrderNext) {
                    pairNextArray[(int) i] = true;
                }
            }
            log.info("Time spent in order {}: {}", curOrder, System.currentTimeMillis() - startTime);
        }

        List<APLExplanationResult> results = new ArrayList<>();
        for (int curOrder: savedAggregates.keySet()) {
            Long2ObjectOpenHashMap<double []> curOrderSavedAggregates = savedAggregates.get(curOrder);
            for (long curSet : curOrderSavedAggregates.keySet()) {
                double[] aggregates = curOrderSavedAggregates.get(curSet);
                double[] metrics = new double[qualityMetrics.length];
                for (int i = 0; i < metrics.length; i++) {
                    metrics[i] = qualityMetrics[i].value(aggregates);
                }
                results.add(
                        new APLExplanationResult(qualityMetrics, curSet, aggregates, metrics, logCardinality)
                );
            }
        }
        return results;
    }

    private boolean[] precalculateCandidates(int curOrder, long logCardinality) {
        if (curOrder < 3) {
            return null;
        } else {
            return getOrder3Candidates(
                    setNext.get(2),
                    singleNext,
                    logCardinality
            );
        }
    }

    private boolean[] getOrder3Candidates(
            LongOpenHashSet o2Candidates,
            LongOpenHashSet singleCandidates,
            long logCardinality
    ) {
        final int ceilCardinality = Math.toIntExact(Math.round(Math.pow(2, logCardinality)));
        LongOpenHashSet candidates = new LongOpenHashSet(o2Candidates.size() * singleCandidates.size() / 2);
        for (long pCandidate : o2Candidates) {
            for (long sCandidate : singleCandidates) {
                if (!IntSetAsLong.contains(pCandidate, sCandidate, logCardinality)) {
                    long nCandidate = IntSetAsLong.threeIntToLong(IntSetAsLong.getFirst(pCandidate,
                            logCardinality), IntSetAsLong.getSecond(pCandidate, logCardinality),
                            sCandidate, logCardinality);
                    candidates.add(nCandidate);
                }
            }
        }

        boolean[] finalCandidates = new boolean[ceilCardinality * ceilCardinality * ceilCardinality];
        long subPair;
        for (long curCandidate : candidates) {
            subPair = IntSetAsLong.twoIntToLong(IntSetAsLong.getFirst(curCandidate, logCardinality),
                    IntSetAsLong.getSecond(curCandidate, logCardinality),
                    logCardinality);
            if (o2Candidates.contains(subPair)) {
                subPair = IntSetAsLong.twoIntToLong(IntSetAsLong.getSecond(curCandidate, logCardinality),
                        IntSetAsLong.getThird(curCandidate, logCardinality),
                        logCardinality);
                if (o2Candidates.contains(subPair)) {
                    subPair = IntSetAsLong.twoIntToLong(IntSetAsLong.getFirst(curCandidate, logCardinality),
                            IntSetAsLong.getThird(curCandidate, logCardinality),
                            logCardinality);
                    if (o2Candidates.contains(subPair)) {
                        finalCandidates[(int) curCandidate] = true;
                    }
                }
            }
        }

        return finalCandidates;
    }

    /**
     * Returns all candidate subsets of a given set and order
     * @param order size of the subsets
     * @return all subsets that can be built from smaller subsets in setNext
     */
    private ArrayList<Long> getCandidates(
            int order,
            int[] set,
            boolean[] precalculatedCandidates,
            long logCardinality
    ) {
        ArrayList<Long> candidates = new ArrayList<>();
        if (order == 1) {
            for (long i : set) {
                if (i != AttributeEncoder.noSupport)
                    candidates.add(i);
            }
        } else {
            ArrayList<Integer> toExamine = new ArrayList<>();
            for (int v : set) {
                if (v != AttributeEncoder.noSupport && singleNextArray[v]) {
                    toExamine.add(v);
                }
            }
            int numValidSingles = toExamine.size();

            if (order == 2) {
                for (int p1 = 0; p1 < numValidSingles; p1++) {
                    int p1v = toExamine.get(p1);
                    for (int p2 = p1 + 1; p2 < numValidSingles; p2++) {
                        int p2v = toExamine.get(p2);
                        candidates.add(IntSetAsLong.twoIntToLong(p1v, p2v, logCardinality));
                    }
                }
            } else if (order == 3) {
                for (int p1 = 0; p1 < numValidSingles; p1++) {
                    int p1v = toExamine.get(p1);
                    for (int p2 = p1 + 1; p2 < numValidSingles; p2++) {
                        int p2v = toExamine.get(p2);
                        long pair1 = IntSetAsLong.twoIntToLong(p1v, p2v, logCardinality);
                        if (pairNextArray[(int) pair1]) {
                            for (int p3 = p2 + 1; p3 < numValidSingles; p3++) {
                                int p3v = toExamine.get(p3);
                                long curSet = IntSetAsLong.threeIntToLong(p1v, p2v, p3v, logCardinality);
                                if (precalculatedCandidates[(int) curSet]) {
                                    candidates.add(curSet);
                                }
                            }
                        }
                    }
                }
            } else {
                throw new MacrobaseInternalError("High Order not supported");
            }
        }
        return candidates;
    }
}
