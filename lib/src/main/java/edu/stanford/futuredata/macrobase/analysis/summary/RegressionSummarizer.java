package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.summary.itemset.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.IntSet;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.ItemsetResult;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Finds itemsets whose correlated metrics has a mean that is far away from the
 * global mean.
 */
public class RegressionSummarizer implements Operator<DataFrame, Explanation> {
    Logger log = LoggerFactory.getLogger("Regression");

    int numRows;
    AttributeEncoder encoder;
    List<String> attributes = new ArrayList<>();

    int numEvents;
    int minCount;
    double minStd;
    int minThreshold;

    String countColumn = "count";
    String meanColumn = "mean";
    String maxColumn = "max";

    int numSingles;

    Set<Integer> singleNext;

    HashMap<Integer, HashMap<IntSet, Integer>> setIdxMapping;
    HashMap<Integer, HashSet<IntSet>> setSaved;
    HashMap<Integer, HashSet<IntSet>> setNext;
    HashMap<Integer, int[]> setCounts;
    HashMap<Integer, int[]> setSums;
    HashMap<Integer, int[]> setMaxs;

    long[] timings = new long[4];

    public RegressionSummarizer() {
        setIdxMapping = new HashMap<>();
        setSaved = new HashMap<>();
        setNext = new HashMap<>();
        setCounts = new HashMap<>();
        setSums = new HashMap<>();
        setMaxs = new HashMap<>();
    }

    @Override
    public void process(DataFrame input) throws Exception {
        numRows = input.getNumRows();

        // Marking Outliers
        double[] countCol = null;
        if (input.hasColumn(countColumn)) {
            countCol = input.getDoubleColumnByName(countColumn);
        }
        double[] meanCol = input.getDoubleColumnByName(meanColumn);
        double[] maxCol = input.getDoubleColumnByName(maxColumn);
        numEvents = 0;
        if (countCol != null) {
            for (int i = 0; i < numRows; i++) {
                numEvents += countCol[i];
            }
        } else {
            numEvents = numRows;
        }

        // Encoding
        encoder = new AttributeEncoder();
        encoder.setColumnNames(attributes);
        long startTime = System.currentTimeMillis();
        List<int[]> encoded = encoder.encodeAttributes(
                input.getStringColsByName(attributes)
        );
        long elapsed = System.currentTimeMillis() - startTime;
        numSingles = encoder.getNextKey();
        log.debug("Encoded in: {}", elapsed);
        log.debug("Encoded Categories: {}", encoder.getNextKey());

        countSingles(
                encoded,
                countCol,
                meanCol,
                maxCol
        );

        countSet(
                encoded,
                countCol,
                meanCol,
                maxCol,
                2
        );

        countSet(
                encoded,
                countCol,
                meanCol,
                maxCol,
                3
        );

        for (int o = 1; o <= 3; o++) {
            log.info("Order {} Explanations: {}", o, setSaved.get(o).size());
        }

    }

    private void countSet(List<int[]> encoded, double[] countCol, double[] meanCol, double[] maxCol, int order) {
        log.debug("Processing Order {}", order);
        long startTime = System.currentTimeMillis();
        HashMap<IntSet, Integer> setMapping = new HashMap<>();
        int maxSetIdx = 0;
        int maxSets = 0;
        if (order == 2) {
            maxSets = singleNext.size() * singleNext.size() / 2;
        } else {
            maxSets = setNext.get(order-1).size() * singleNext.size();
        }
        int[] counts = new int[maxSets];
        double[] sums = new double[maxSets];
        double[] maxs = new double[maxSets];

        boolean hasCountCol = countCol != null;
        for (int i = 0; i < numRows; i++) {
            int[] curRow = encoded.get(i);
            ArrayList<Integer> toExamine = new ArrayList<>();
            for (int v : curRow) {
                if (singleNext.contains(v)) {
                    toExamine.add(v);
                }
            }
            int l = toExamine.size();

            ArrayList<IntSet> setsToAdd = new ArrayList<>();
            if (order == 2) {
                for (int p1 = 0; p1 < l; p1++) {
                    int p1v = toExamine.get(p1);
                    for (int p2 = p1 + 1; p2 < l; p2++) {
                        int p2v = toExamine.get(p2);
                        setsToAdd.add(new IntSet(p1v, p2v));
                    }
                }
            } else if (order == 3) {
                HashSet<IntSet> pairNext = setNext.get(2);
                for (int p1 = 0; p1 < l; p1++) {
                    int p1v = toExamine.get(p1);
                    for (int p2 = p1+1; p2 < l; p2++) {
                        int p2v = toExamine.get(p2);
                        IntSet pair1 = new IntSet(p1v, p2v);
                        if (pairNext.contains(pair1)) {
                            for (int p3 = p2 + 1; p3 < l; p3++) {
                                int p3v = toExamine.get(p3);
                                setsToAdd.add(new IntSet(p1v, p2v, p3v));
                            }
                        }
                    }
                }
            }

            for (IntSet curSet : setsToAdd) {
                int setIdx = setMapping.getOrDefault(curSet, -1);
                if (setIdx < 0) {
                    setIdx = maxSetIdx;
                    setMapping.put(curSet, setIdx);
                    maxSetIdx++;
                }
                counts[setIdx] += hasCountCol ? countCol[i] : 1;
                sums[setIdx] += countCol[i] * meanCol[i];
                if (maxs[setIdx] < maxCol[i]) {
                    maxs[setIdx] = maxCol[i];
                }
            }
        }
        long elapsed = System.currentTimeMillis() - startTime;
        timings[order] = elapsed;
        log.debug("Counted order {} in: {}", order, elapsed);

        HashSet<IntSet> saved = new HashSet<>();
        int numPruned = 0;
        HashSet<IntSet> next = new HashSet<>();
        for (IntSet curSet : setMapping.keySet()) {
            int setIdx = setMapping.get(curSet);
            int count = counts[setIdx];
            double sum = sums[setIdx];
            double max = maxs[setIdx];
            if (count < minCount) {
                numPruned++;
            } else if (max < minThreshold) {
                numPruned++;
            } else if (sum / minThreshold < minCount) {
                numPruned++;
            } else {
                double mean = sum / count;
                if (mean > minThreshold) {
                    saved.add(curSet);
                } else {
                    next.add(curSet);
                }
            }
        }

        log.debug("Itemsets Saved: {}", saved.size());
        log.debug("Itemsets Pruned: {}", numPruned);
        log.debug("Itemsets Next: {}", next.size());

        setIdxMapping.put(order, setMapping);
        setSaved.put(order, saved);
        setNext.put(order, next);
        setCounts.put(order, counts);
    }

    private void countSingles(List<int[]> encoded, double[] countCol, double[] meanCol, double[] maxCol) {
        // Counting Singles
        long startTime = System.currentTimeMillis();
        int[] singleCounts = new int[numSingles];
        double[] singleSums = new double[numSingles];
        double[] singleMaxs = new double[numSingles];
        double sum = 0.0;
        double squaredSum = 0.0;
        boolean hasCountCol = countCol != null;
        for (int i = 0; i < numRows; i++) {
            int[] curRow = encoded.get(i);
            for (int v : curRow) {
                singleCounts[v] += hasCountCol ? countCol[i] : 1;
                singleSums[v] += countCol[i] * meanCol[i];
                if (singleMaxs[v] < maxCol[i]) {
                    singleMaxs[v] = maxCol[i];
                }
            }
            sum += countCol[i] * meanCol[i];
            squaredSum += countCol[i] * meanCol[i] * countCol[i] * meanCol[i];
        }
        long elapsed = System.currentTimeMillis() - startTime;
        timings[1] = elapsed;
        double globalMean = sum / numEvents;
        double globalStd = squaredSum / numEvents - (globalMean * globalMean);
        minThreshold = (int)(globalMean + minStd * globalStd);
        log.info("Min Support Count: {}", minCount);
        log.info("Min Threshold: {}", minThreshold);
        log.debug("Counted Singles in: {}", elapsed);

        HashSet<Integer> singleSaved = new HashSet<>();
        singleNext = new HashSet<>();
        int numPruned = 0;
        for (int i = 0; i < numSingles; i++) {
            if (singleCounts[i] < minCount) {
                numPruned++;
            } else if (singleMaxs[i] < minThreshold) {
                numPruned++;
            } else if (singleSums[i] / minThreshold < minCount) {
                numPruned++;
            } else {
                double mean = singleSums[i] / singleCounts[i];
                if (mean > minThreshold) {
                    singleSaved.add(i);
                } else {
                    singleNext.add(i);
                }
            }
        }
        log.debug("Itemsets Saved: {}", singleSaved.size());
        log.debug("Itemsets Pruned: {}", numPruned);
        log.debug("Itemsets Next: {}", singleNext.size());

        HashMap<IntSet, Integer> curIdxMapping = new HashMap<>(numSingles);
        HashSet<IntSet> curSaved = new HashSet<>(singleSaved.size());
        HashSet<IntSet> curNext = new HashSet<>(singleNext.size());
        for (int i = 0; i < numSingles; i++) {
            curIdxMapping.put(new IntSet(i), i);
        }
        for (int i : singleSaved) {
            curSaved.add(new IntSet(i));
        }
        for (int i : singleNext) {
            curNext.add(new IntSet(i));
        }

        setIdxMapping.put(1, curIdxMapping);
        setSaved.put(1, curSaved);
        setNext.put(1, curNext);
        setCounts.put(1, singleCounts);
    }

    @Override
    public Explanation getResults() {
        List<AttributeSet> results = new ArrayList<>();
        for (int o = 1; o <= 3; o++) {
            HashSet<IntSet> curResults = setSaved.get(o);
            HashMap<IntSet, Integer> idxMapping = setIdxMapping.get(o);
            int[] counts = setCounts.get(o);
            for (IntSet vs : curResults) {
                int idx = idxMapping.get(vs);
                int count = counts[idx];
                ItemsetResult iResult = new ItemsetResult(
                        0,
                        count,
                        0,
                        vs.getSet()
                );
                AttributeSet aSet = new AttributeSet(iResult, encoder);
                results.add(aSet);
            }
        }
        Explanation finalExplanation = new Explanation(
                results,
                0,
                0,
                timings[1]+timings[2]+timings[3]
        );
        return finalExplanation;
    }

    /**
    * Set the column which indicates the number of raw rows in each cubed group.
    * @param countColumn count column.
    * @return this
    */
    public RegressionSummarizer setCountColumn(String countColumn) {
        this.countColumn = countColumn;
        return this;
    }

    public RegressionSummarizer setMeanColumn(String meanColumn) {
        this.meanColumn = meanColumn;
        return this;
    }

    public RegressionSummarizer setMaxColumn(String maxColumn) {
        this.maxColumn = maxColumn;
        return this;
    }

    public RegressionSummarizer setAttributes(List<String> attributes) {
        this.attributes = attributes;
        return this;
    }

    public RegressionSummarizer setMinCount(int minCount) {
        this.minCount = minCount;
        return this;
    }

    public RegressionSummarizer setMinStd(double minStd) {
        this.minStd = minStd;
        return this;
    }
}
