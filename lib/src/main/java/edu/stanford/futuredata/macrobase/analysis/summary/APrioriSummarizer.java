package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.summary.itemset.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.IntSet;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.ItemsetResult;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Simple, direct itemset mining with pruning that is limited to low-order
 * interactions.
 */
public class APrioriSummarizer extends BatchSummarizer {
    Logger log = LoggerFactory.getLogger("APriori");

    int numRows;
    AttributeEncoder encoder;

    int numEvents;
    int numOutliers;
    double baseRate;
    int suppCount;

    String countColumn = "count";

    int numSingles;

    HashSet<Integer> singleNext;

    HashMap<Integer, HashMap<IntSet, Integer>> setIdxMapping;
    // sets that have high risk ratio and no longer need to be explored
    HashMap<Integer, HashSet<IntSet>> setSaved;
    // sets that has high enough support but not high risk ratio, need to be explored
    HashMap<Integer, HashSet<IntSet>> setNext;
    HashMap<Integer, int[]> setCounts;
    HashMap<Integer, int[]> setOCounts;

    long[] timings = new long[4];

    public APrioriSummarizer() {
        setIdxMapping = new HashMap<>();
        setSaved = new HashMap<>();
        setNext = new HashMap<>();
        setCounts = new HashMap<>();
        setOCounts = new HashMap<>();
    }

    @Override
    public void process(DataFrame input) throws Exception {
        numRows = input.getNumRows();

        // Marking Outliers
        double[] outlierCol = input.getDoubleColumnByName(outlierColumn);
        double[] countCol = null;
        if (input.hasColumn(countColumn)) {
            countCol = input.getDoubleColumnByName(countColumn);
        }
        numEvents = 0;
        if (countCol != null) {
            for (int i = 0; i < numRows; i++) {
                numEvents += countCol[i];
            }
        } else {
            numEvents = numRows;
        }
        double numOutliersExact = 0.0;
        for (int i = 0; i < numRows; i++) {
            numOutliersExact += outlierCol[i];
        }
        numOutliers = (int) numOutliersExact;
        baseRate = numOutliersExact*1.0/numEvents;
        suppCount = (int) (minOutlierSupport * numOutliers);
        log.info("Outliers: {}", numOutliers);
        log.info("Outlier Rate of: {}", baseRate);
        log.info("Min Support Count: {}", suppCount);
        log.info("Min Risk Ratio: {}", minRiskRatio);

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
                outlierCol
        );

        countSet(
                encoded,
                countCol,
                outlierCol,
                2
        );

        countSet(
                encoded,
                countCol,
                outlierCol,
                3
        );

        for (int o = 1; o <= 3; o++) {
            log.info("Order {} Explanations: {}", o, setSaved.get(o).size());
        }

    }

    public static HashSet<IntSet> getOrder3Candidates(
            HashSet<IntSet> o2Candidates,
            HashSet<Integer> singleCandidates
            ) {
        HashSet<IntSet> candidates = new HashSet<>(o2Candidates.size() * singleCandidates.size() / 2);
        for (IntSet pCandidate : o2Candidates) {
            for (int sCandidate : singleCandidates) {
                if (!pCandidate.contains(sCandidate)) {
                    IntSet nCandidate = new IntSet(pCandidate.values[0], pCandidate.values[1], sCandidate);
                    candidates.add(nCandidate);
                }
            }
        }

        HashSet<IntSet> finalCandidates = new HashSet<>(candidates.size());
        IntSet subPair;
        for (IntSet curCandidate : candidates) {
            subPair = new IntSet(curCandidate.values[0], curCandidate.values[1]);
            if (o2Candidates.contains(subPair)) {
                subPair = new IntSet(curCandidate.values[1], curCandidate.values[2]);
                if (o2Candidates.contains(subPair)) {
                    subPair = new IntSet(curCandidate.values[0], curCandidate.values[2]);
                    if (o2Candidates.contains(subPair)) {
                        finalCandidates.add(curCandidate);
                    }
                }
            }
        }

        return finalCandidates;
    }

    private void countSet(List<int[]> encoded, double[] countCol, double[] outlierCol, int order) {
        log.debug("Processing Order {}", order);
        long startTime = System.currentTimeMillis();
        // Map each integer set under consideration an index so we can count using arrays
        HashMap<IntSet, Integer> setMapping = new HashMap<>();
        int maxSetIdx = 0;
        int maxSets = 0;
        if (order == 2) {
            maxSets = singleNext.size() * singleNext.size() / 2;
        } else {
            maxSets = setNext.get(order-1).size() * singleNext.size();
        }
        int[] oCounts = new int[maxSets];
        int[] counts = new int[maxSets];
        HashSet<IntSet> candidates = new HashSet<>();
        if (order == 3) {
            // candidate triplets are built from 3 pairs all of which are unpruned / unsaved
            candidates = getOrder3Candidates(setNext.get(2), singleNext);
        }

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
                                IntSet curSet = new IntSet(p1v, p2v, p3v);
                                if (candidates.contains(curSet)) {
                                    setsToAdd.add(curSet);
                                }
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
                oCounts[setIdx] += outlierCol[i];
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
            int oCount = oCounts[setIdx];
            int count = counts[setIdx];
            if (oCount < suppCount) {
                numPruned++;
            } else {
                double ratio = oCount * 1.0 / (count * baseRate);
                if (ratio > minRiskRatio) {
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
        setOCounts.put(order, oCounts);
    }

    private void countSingles(List<int[]> encoded, double[] countCol, double[] outlierCol) {
        // Counting Singles
        long startTime = System.currentTimeMillis();
        int[] singleCounts = new int[numSingles];
        int[] singleOCounts = new int[numSingles];
        boolean hasCountCol = countCol != null;
        for (int i = 0; i < numRows; i++) {
            int[] curRow = encoded.get(i);
            for (int v : curRow) {
                singleCounts[v] += hasCountCol ? countCol[i] : 1;
                singleOCounts[v] += outlierCol[i];
            }
        }
        long elapsed = System.currentTimeMillis() - startTime;
        timings[1] = elapsed;
        log.debug("Counted Singles in: {}", elapsed);

        HashSet<Integer> singleSaved = new HashSet<>();
        singleNext = new HashSet<>();
        int numPruned = 0;
        for (int i = 0; i < numSingles; i++) {
            if (singleOCounts[i] < suppCount) {
                numPruned++;
            } else {
                double ratio = singleOCounts[i]*1.0 / (singleCounts[i] * baseRate);
                if (ratio > minRiskRatio) {
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
        setOCounts.put(1, singleOCounts);
    }

    @Override
    public Explanation getResults() {
        List<AttributeSet> results = new ArrayList<>();
        for (int o = 1; o <= 3; o++) {
            HashSet<IntSet> curResults = setSaved.get(o);
            HashMap<IntSet, Integer> idxMapping = setIdxMapping.get(o);
            int[] oCounts = setOCounts.get(o);
            int[] counts = setCounts.get(o);
            for (IntSet vs : curResults) {
                int idx = idxMapping.get(vs);
                int oCount = oCounts[idx];
                int count = counts[idx];
                double lift = (oCount*1.0/count) / baseRate;
                double support = oCount*1.0 / numOutliers;
                ItemsetResult iResult = new ItemsetResult(
                        support,
                        count,
                        lift,
                        vs.getSet()
                );
                AttributeSet aSet = new AttributeSet(iResult, encoder);
                results.add(aSet);
            }
        }
        Explanation finalExplanation = new Explanation(
                results,
                numEvents - numOutliers,
                numOutliers,
                timings[1]+timings[2]+timings[3]
        );
        finalExplanation.sortBySupport();
        return finalExplanation;
    }

    /**
    * Set the column which indicates the number of raw rows in each cubed group.
    * @param countColumn count column.
    * @return this
    */
    public APrioriSummarizer setCountColumn(String countColumn) {
        this.countColumn = countColumn;
        return this;
    }
}
