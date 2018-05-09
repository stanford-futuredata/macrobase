package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.*;
import edu.stanford.futuredata.macrobase.util.MacroBaseInternalError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class for handling the generic, algorithmic aspects of apriori explanation.
 * This class assumes that subgroups posses "aggregates" such as count and outlier_count
 * which can be combined additively. Then, we use APriori to find the subgroups which
 * are the most interesting as defined by "quality metrics" on these aggregates.
 */
public class APrioriBasic extends APriori {
    Logger log = LoggerFactory.getLogger("APrioriBasic");

    // **Parameters**
    private QualityMetric[] qualityMetrics;
    private double[] thresholds;

    private double injectFraction = 0.0;
    private Random rand;
    private boolean smartStopping = true;
    private boolean doContainment = true;
    private int maxOrder = 3;

    // **Cached values**

    // singleton viable sets for quick lookup
    HashSet<Integer> singleNext;
    // sets that has high enough support but not high risk ratio, need to be explored
    HashMap<Integer, HashSet<IntSetAsArray>> setNext;
    // aggregate values for all of the sets we saved
    HashMap<Integer, HashMap<IntSetAsArray, double[]>> savedAggregates;

    public APrioriBasic(
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
            int[][] attributes,
            double[][] aggregateColumns
    ) {
        int d = aggregateColumns.length;
        int n = aggregateColumns[0].length;

        long start;

        // Quality metrics are initialized with global aggregates to
        // allow them to determine the appropriate relative thresholds
        start = System.nanoTime();
        double[] globalAggregates = new double[d];
        for (int j = 0; j < d; j++) {
            globalAggregates[j] = 0;
            double[] curColumn = aggregateColumns[j];
            for (int i = 0; i < n; i++) {
                globalAggregates[j] += curColumn[i];
            }
        }
        for (QualityMetric q : qualityMetrics) {
            q.initialize(globalAggregates);
        }
        initializationTime = (System.nanoTime() - start) / 1.e6;

        // row store for more convenient access
        start = System.nanoTime();
        double[][] aRows = new double[n][d];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < d; j++) {
                aRows[i][j] = aggregateColumns[j][i];
            }
        }
        rowstoreTime = (System.nanoTime() - start) / 1.e6;

        for (int curOrder = 1; curOrder <= maxOrder; curOrder++) {
            long orderStart = System.nanoTime();
            // Group by and calculate aggregates for each of the candidates
            HashMap<IntSetAsArray, double[]> setAggregates = new HashMap<>();

            // precalculate all possible candidate sets from "next" sets of
            // previous orders. We will focus on aggregating results for these
            // sets.
            start = System.nanoTime();
            HashSet<IntSetAsArray> precalculatedCandidates = precalculateCandidates(curOrder);
            for (int i = 0; i < n; i++){
                int[] curRowAttributes = attributes[i];
                ArrayList<IntSetAsArray> candidates = getCandidates(
                        curOrder,
                        curRowAttributes,
                        precalculatedCandidates
                );
                for (IntSetAsArray curCandidate : candidates) {
                    if (!setAggregates.containsKey(curCandidate)) {
                        setAggregates.put(curCandidate, new double[d]);
                    }
                    setAggregates.merge(
                            curCandidate,
                            aRows[i],
                            APrioriBasic::addToArray
                    );
                }
                numProcessed[curOrder - 1] += candidates.size();
            }
            aggregationTime[curOrder - 1] = (System.nanoTime() - start) / 1.e6;

            start = System.nanoTime();
            HashSet<IntSetAsArray> curOrderNext = new HashSet<>();
            HashSet<IntSetAsArray> curOrderSaved = new HashSet<>();
            int pruned = 0;
            for (IntSetAsArray curCandidate: setAggregates.keySet()) {
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
                        numSaved[curOrder - 1]++;
                    }

                    if (!doContainment || !isPastThreshold) {
                        // otherwise if a set still has potentially good subsets,
                        // save it for further examination
                        curOrderNext.add(curCandidate);
                        numNext[curOrder - 1]++;
                    }
                } else {
                    if (injectFraction > 0.0 && rand.nextDouble() < injectFraction) {
                        curOrderNext.add(curCandidate);
                        numNext[curOrder - 1]++;
                    } else {
                        pruned++;
                    }
                }
            }
            pruneTime[curOrder - 1] = (System.nanoTime() - start) / 1.e6;

            start = System.nanoTime();
            HashMap<IntSetAsArray, double[]> curSavedAggregates = new HashMap<>(curOrderSaved.size());
            for (IntSetAsArray curSaved : curOrderSaved) {
                curSavedAggregates.put(curSaved, setAggregates.get(curSaved));
            }
            savedAggregates.put(curOrder, curSavedAggregates);
            setNext.put(curOrder, curOrderNext);
            if (curOrder == 1) {
                singleNext = new HashSet<>(curOrderNext.size());
                for (IntSetAsArray i : curOrderNext) {
                    singleNext.add(i.getFirst());
                }
            }
            saveTime[curOrder - 1] = (System.nanoTime() - start) / 1.e6;
            explainTime[curOrder - 1] = (System.nanoTime() - orderStart) / 1.e6;
            if (smartStopping && curOrderNext.isEmpty()) {
                break;
            }
        }

        List<APLExplanationResult> results = new ArrayList<>();
        for (int curOrder: savedAggregates.keySet()) {
            HashMap<IntSetAsArray, double[]> curOrderSavedAggregates = savedAggregates.get(curOrder);
            for (IntSetAsArray curSet : curOrderSavedAggregates.keySet()) {
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

    private static double[] addToArray(double[] a, double[] b) {
        for (int i = 0; i < a.length; i++) {
            a[i] += b[i];
        }
        return a;
    }

    private HashSet<IntSetAsArray> precalculateCandidates(int curOrder) {
        if (curOrder < 3) {
            return null;
        } else {
            return getOrder3Candidates(
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
    private ArrayList<IntSetAsArray> getCandidates(
            int order,
            int[] set,
            HashSet<IntSetAsArray> precalculatedCandidates
    ) {
        ArrayList<IntSetAsArray> candidates = new ArrayList<>();
        if (order == 1) {
            for (int i : set) {
                candidates.add(new IntSetAsArray(i));
            }
        } else {
            ArrayList<Integer> toExamine = new ArrayList<>();
            for (int v : set) {
                if (singleNext.contains(v)) {
                    toExamine.add(v);
                }
            }
            int l = toExamine.size();

            if (order == 2) {
                for (int p1 = 0; p1 < l; p1++) {
                    int p1v = toExamine.get(p1);
                    for (int p2 = p1 + 1; p2 < l; p2++) {
                        int p2v = toExamine.get(p2);
                        candidates.add(new IntSetAsArray(p1v, p2v));
                    }
                }
            } else if (order == 3) {
                HashSet<IntSetAsArray> pairNext = setNext.get(2);
                for (int p1 = 0; p1 < l; p1++) {
                    int p1v = toExamine.get(p1);
                    for (int p2 = p1 + 1; p2 < l; p2++) {
                        int p2v = toExamine.get(p2);
                        IntSetAsArray pair1 = new IntSetAsArray(p1v, p2v);
                        if (pairNext.contains(pair1)) {
                            for (int p3 = p2 + 1; p3 < l; p3++) {
                                int p3v = toExamine.get(p3);
                                IntSetAsArray curSet = new IntSetAsArray(p1v, p2v, p3v);
                                if (precalculatedCandidates.contains(curSet)) {
                                    candidates.add(curSet);
                                }
                            }
                        }
                    }
                }
            } else {
                throw new MacroBaseInternalError("High Order not supported");
            }
        }
        return candidates;
    }

    public static HashSet<IntSetAsArray> getOrder3Candidates(
            HashSet<IntSetAsArray> o2Candidates,
            HashSet<Integer> singleCandidates
    ) {
        HashSet<IntSetAsArray> candidates = new HashSet<>(o2Candidates.size() * singleCandidates.size() / 2);
        for (IntSetAsArray pCandidate : o2Candidates) {
            for (int sCandidate : singleCandidates) {
                if (!pCandidate.contains(sCandidate)) {
                    IntSetAsArray nCandidate = new IntSetAsArray(pCandidate.getFirst(), pCandidate.getSecond(), sCandidate);
                    candidates.add(nCandidate);
                }
            }
        }

        HashSet<IntSetAsArray> finalCandidates = new HashSet<>(candidates.size());
        IntSetAsArray subPair;
        for (IntSetAsArray curCandidate : candidates) {
            subPair = new IntSetAsArray(curCandidate.getFirst(), curCandidate.getSecond());
            if (o2Candidates.contains(subPair)) {
                subPair = new IntSetAsArray(curCandidate.getSecond(), curCandidate.getThird());
                if (o2Candidates.contains(subPair)) {
                    subPair = new IntSetAsArray(curCandidate.getFirst(), curCandidate.getThird());
                    if (o2Candidates.contains(subPair)) {
                        finalCandidates.add(curCandidate);
                    }
                }
            }
        }

        return finalCandidates;
    }

    public void setInjectFraction(double injectFraction) {
        if (injectFraction > 0.0) {
            this.injectFraction = injectFraction;
            rand = new Random();
        }
    }

    public void setSmartStopping(boolean smartStopping) { this.smartStopping = smartStopping; }
    public void setDoContainment(boolean doContainment) { this.doContainment = doContainment; }
    public void setMaxOrder(int maxOrder) { this.maxOrder = maxOrder; }
}
