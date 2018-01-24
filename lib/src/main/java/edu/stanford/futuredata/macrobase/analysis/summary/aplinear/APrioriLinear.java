package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

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
    // Sets that has high enough support but not high risk ratio, need to be explored
    private HashMap<Integer, LongOpenHashSet> setNext;
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
            double[][] aggregateColumns
    ) {
        final int numAggregates = aggregateColumns.length;
        final int numRows = aggregateColumns[0].length;

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
            // Precalculate all possible candidate sets from "next" sets of
            // previous orders. We will focus on aggregating results for these
            // sets.
            final LongOpenHashSet precalculatedCandidates = precalculateCandidates(curOrder);
            // Run the critical path of the algorithm--candidate generation--in parallel.
            final int curOrderFinal = curOrder;
            final int numThreads = Runtime.getRuntime().availableProcessors();
            // Group by and calculate aggregates for each of the candidates
            final ArrayList<Long2ObjectOpenHashMap<double []>> threadSetAggregates = new ArrayList<>(numThreads);
            for (int i = 0; i < numThreads; i++) {
                threadSetAggregates.add(new Long2ObjectOpenHashMap<>());
            }
            final CountDownLatch doneSignal = new CountDownLatch(numThreads);
            for (int threadNum = 0; threadNum < numThreads; threadNum++) {
                final int startIndex = (numRows * threadNum) / numThreads;
                final int endIndex = (numRows * (threadNum + 1)) / numThreads;
                final Long2ObjectOpenHashMap<double []> thisThreadSetAggregates = threadSetAggregates.get(threadNum);
                // Do the critical path calculation in a lambda
                Runnable APrioriLinearRunnable = () -> {
                        for (int i = startIndex; i < endIndex; i++) {
                            int[] curRowAttributes = attributes[i];
                            ArrayList<Long> candidates = getCandidates(
                                    curOrderFinal,
                                    curRowAttributes,
                                    precalculatedCandidates
                            );
                            int numCandidates = candidates.size();
                            for (int c = 0; c < numCandidates; c++) {
                                long curCandidate = candidates.get(c);
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
                        doneSignal.countDown();
                };
                // Run numThreads lambdas in separate threads
                Thread APrioriLinearThread = new Thread(APrioriLinearRunnable);
                APrioriLinearThread.start();
            }
            try {
                doneSignal.await();
            } catch (InterruptedException ex) {ex.printStackTrace();}

            // Collect the threadSetAggregates into one big set of aggregates.
            Long2ObjectOpenHashMap<double []> setAggregates = new Long2ObjectOpenHashMap<>();
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

            LongOpenHashSet curOrderNext = new LongOpenHashSet();
            LongOpenHashSet curOrderSaved = new LongOpenHashSet();
            int pruned = 0;
            for (long curCandidate: setAggregates.keySet()) {
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

            Long2ObjectOpenHashMap<double []> curSavedAggregates = new Long2ObjectOpenHashMap(curOrderSaved.size());
            for (long curSaved : curOrderSaved) {
                curSavedAggregates.put(curSaved, setAggregates.get(curSaved));
            }
            savedAggregates.put(curOrder, curSavedAggregates);
            setNext.put(curOrder, curOrderNext);
            if (curOrder == 1) {
                singleNext = new LongOpenHashSet(curOrderNext.size());
                for (long i : curOrderNext) {
                    singleNext.add(i);
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
                        new APLExplanationResult(qualityMetrics, curSet, aggregates, metrics)
                );
            }
        }
        return results;
    }

    private LongOpenHashSet precalculateCandidates(int curOrder) {
        if (curOrder < 3) {
            return null;
        } else {
            return getOrder3Candidates(
                    setNext.get(2),
                    singleNext
            );
        }
    }

    public static LongOpenHashSet getOrder3Candidates(
            LongOpenHashSet o2Candidates,
            LongOpenHashSet singleCandidates
    ) {
        LongOpenHashSet candidates = new LongOpenHashSet(o2Candidates.size() * singleCandidates.size() / 2);
        for (long pCandidate : o2Candidates) {
            for (long sCandidate : singleCandidates) {
                if (!IntSetAsLong.contains(pCandidate, sCandidate)) {
                    long nCandidate = IntSetAsLong.threeIntToLong(IntSetAsLong.getFirst(pCandidate), IntSetAsLong.getSecond(pCandidate), sCandidate);
                    candidates.add(nCandidate);
                }
            }
        }

        LongOpenHashSet finalCandidates = new LongOpenHashSet(candidates.size());
        long subPair;
        for (long curCandidate : candidates) {
            subPair = IntSetAsLong.twoIntToLong(IntSetAsLong.getFirst(curCandidate), IntSetAsLong.getSecond(curCandidate));
            if (o2Candidates.contains(subPair)) {
                subPair = IntSetAsLong.twoIntToLong(IntSetAsLong.getSecond(curCandidate), IntSetAsLong.getThird(curCandidate));
                if (o2Candidates.contains(subPair)) {
                    subPair = IntSetAsLong.twoIntToLong(IntSetAsLong.getFirst(curCandidate), IntSetAsLong.getThird(curCandidate));
                    if (o2Candidates.contains(subPair)) {
                        finalCandidates.add(curCandidate);
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
            LongOpenHashSet precalculatedCandidates
    ) {
        ArrayList<Long> candidates = new ArrayList<>();
        if (order == 1) {
            for (long i : set) {
                candidates.add(i);
            }
        } else {
            ArrayList<Integer> toExamine = new ArrayList<>();
            for (int v : set) {
                if (singleNext.contains(v)) {
                    toExamine.add(v);
                }
            }
            int numValidSingles = toExamine.size();

            if (order == 2) {
                for (int p1 = 0; p1 < numValidSingles; p1++) {
                    int p1v = toExamine.get(p1);
                    for (int p2 = p1 + 1; p2 < numValidSingles; p2++) {
                        int p2v = toExamine.get(p2);
                        candidates.add(IntSetAsLong.twoIntToLong(p1v, p2v));
                    }
                }
            } else if (order == 3) {
                LongOpenHashSet pairNext = setNext.get(2);
                for (int p1 = 0; p1 < numValidSingles; p1++) {
                    int p1v = toExamine.get(p1);
                    for (int p2 = p1 + 1; p2 < numValidSingles; p2++) {
                        int p2v = toExamine.get(p2);
                        long pair1 = IntSetAsLong.twoIntToLong(p1v, p2v);
                        if (pairNext.contains(pair1)) {
                            for (int p3 = p2 + 1; p3 < numValidSingles; p3++) {
                                int p3v = toExamine.get(p3);
                                long curSet = IntSetAsLong.threeIntToLong(p1v, p2v, p3v);
                                if (precalculatedCandidates.contains(curSet)) {
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
