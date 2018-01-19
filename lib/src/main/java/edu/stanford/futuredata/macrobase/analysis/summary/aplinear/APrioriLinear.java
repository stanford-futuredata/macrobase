package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.apriori.APrioriSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.apriori.IntSet;
import edu.stanford.futuredata.macrobase.util.MacrobaseInternalError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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
    private HashSet<Integer> singleNext;
    // Sets that has high enough support but not high risk ratio, need to be explored
    private HashMap<Integer, HashSet<IntSet>> setNext;
    // Aggregate values for all of the sets we saved
    private HashMap<Integer, HashMap<IntSet, double[]>> savedAggregates;

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
            final List<int[]> attributes,
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
            // Precalculate all possible candidate sets from "next" sets of
            // previous orders. We will focus on aggregating results for these
            // sets.
            final HashSet<IntSet> precalculatedCandidates = precalculateCandidates(curOrder);
            // Run the critical path of the algorithm--candidate generation--in parallel.
            final int curOrderFinal = curOrder;
            final int numThreads = Runtime.getRuntime().availableProcessors();
            // Group by and calculate aggregates for each of the candidates
            final ArrayList<HashMap<IntSet, double[]>> threadSetAggregates = new ArrayList<>(numThreads);
            for (int i = 0; i < numThreads; i++) {
                threadSetAggregates.add(new HashMap<>());
            }
            final CountDownLatch doneSignal = new CountDownLatch(numThreads);
            for (int threadNum = 0; threadNum < numThreads; threadNum++) {
                final int startIndex = (numRows * threadNum) / numThreads;
                final int endIndex = (numRows * (threadNum + 1)) / numThreads;
                final HashMap<IntSet, double[]> thisThreadSetAggregates = threadSetAggregates.get(threadNum);
                // Do the critical path calculation in a lambda
                Runnable APrioriLinearRunnable = () -> {
                        for (int i = startIndex; i < endIndex; i++) {
                            int[] curRowAttributes = attributes.get(i);
                            ArrayList<IntSet> candidates = getCandidates(
                                    curOrderFinal,
                                    curRowAttributes,
                                    precalculatedCandidates
                            );
                            int numCandidates = candidates.size();
                            for (int c = 0; c < numCandidates; c++) {
                                IntSet curCandidate = candidates.get(c);
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
            HashMap<IntSet, double[]> setAggregates = new HashMap<>();
            for (HashMap<IntSet, double[]> set : threadSetAggregates) {
                for (Map.Entry<IntSet, double[]> curCandidate : set.entrySet()) {
                    IntSet curCandidateKey = curCandidate.getKey();
                    double[] curCandidateValue = curCandidate.getValue();
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

            HashSet<IntSet> curOrderNext = new HashSet<>();
            HashSet<IntSet> curOrderSaved = new HashSet<>();
            int pruned = 0;
            for (IntSet curCandidate: setAggregates.keySet()) {
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

            HashMap<IntSet, double[]> curSavedAggregates = new HashMap<>(curOrderSaved.size());
            for (IntSet curSaved : curOrderSaved) {
                curSavedAggregates.put(curSaved, setAggregates.get(curSaved));
            }
            savedAggregates.put(curOrder, curSavedAggregates);
            setNext.put(curOrder, curOrderNext);
            if (curOrder == 1) {
                singleNext = new HashSet<>(curOrderNext.size());
                for (IntSet i : curOrderNext) {
                    singleNext.add(i.get(0));
                }
            }
        }

        List<APLExplanationResult> results = new ArrayList<>();
        for (int curOrder: savedAggregates.keySet()) {
            HashMap<IntSet, double[]> curOrderSavedAggregates = savedAggregates.get(curOrder);
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

    private HashSet<IntSet> precalculateCandidates(int curOrder) {
        if (curOrder < 3) {
            return null;
        } else {
            return APrioriSummarizer.getOrder3Candidates(
                    setNext.get(2),
                    singleNext
            );
        }
    }

    /**
     * Returns all candidate subsets of a given set and order
     * @param order size of the subsets
     * @return all subsets that can be built from smaller subsets in setNext
     */
    private ArrayList<IntSet> getCandidates(
            int order,
            int[] set,
            HashSet<IntSet> precalculatedCandidates
    ) {
        ArrayList<IntSet> candidates = new ArrayList<>();
        if (order == 1) {
            for (int i : set) {
                candidates.add(new IntSet(i));
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
                        candidates.add(new IntSet(p1v, p2v));
                    }
                }
            } else if (order == 3) {
                HashSet<IntSet> pairNext = setNext.get(2);
                for (int p1 = 0; p1 < numValidSingles; p1++) {
                    int p1v = toExamine.get(p1);
                    for (int p2 = p1 + 1; p2 < numValidSingles; p2++) {
                        int p2v = toExamine.get(p2);
                        IntSet pair1 = new IntSet(p1v, p2v);
                        if (pairNext.contains(pair1)) {
                            for (int p3 = p2 + 1; p3 < numValidSingles; p3++) {
                                int p3v = toExamine.get(p3);
                                IntSet curSet = new IntSet(p1v, p2v, p3v);
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
