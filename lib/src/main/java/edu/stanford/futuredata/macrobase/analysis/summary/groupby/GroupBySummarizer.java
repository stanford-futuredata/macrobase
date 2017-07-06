package edu.stanford.futuredata.macrobase.analysis.summary.groupby;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.AttributeEncoder2;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.*;

public class GroupBySummarizer extends BatchSummarizer {
    private int n, d;

    private double baseRate;
    private int suppCount;

    int numSingles;

    Set<Integer> singleNext;

    HashMap<Integer, HashMap<IntSet, Integer>> setIdxMapping;
    HashMap<Integer, HashSet<IntSet>> setSaved;
    HashMap<Integer, HashSet<IntSet>> setNext;
    HashMap<Integer, int[]> setCounts;
    HashMap<Integer, int[]> setOCounts;

    public GroupBySummarizer() {
        setIdxMapping = new HashMap<>();
        setSaved = new HashMap<>();
        setNext = new HashMap<>();
        setCounts = new HashMap<>();
        setOCounts = new HashMap<>();
    }

    @Override
    public void process(DataFrame input) throws Exception {
        n = input.getNumRows();
        d = attributes.size();

        // Marking Outliers
        boolean[] flag = new boolean[n];
        double[] outlierCol = input.getDoubleColumnByName(outlierColumn);
        int numOutliers = 0;
        for (int i = 0; i < n; i++) {
            flag[i] = predicate.test(outlierCol[i]);
            if (flag[i]) {
                numOutliers++;
            }
        }
        System.out.println("Outliers: "+numOutliers);
        baseRate = numOutliers*1.0/n;
        System.out.println("Base Rate of: "+baseRate);
        suppCount = (int) (minOutlierSupport * n);
        System.out.println("Min Support of: "+suppCount);
        System.out.println("Min RR of: "+minRiskRatio);

        // Encoding
        AttributeEncoder2 encoder = new AttributeEncoder2();
        encoder.setColumnNames(attributes);
        long startTime = System.currentTimeMillis();
        List<int[]> encoded = encoder.encodeAttributes(
                input.getStringColsByName(attributes)
        );
        long elapsed = System.currentTimeMillis() - startTime;
        numSingles = encoder.getNextKey();
        System.out.println("Encoded in: "+elapsed);
        System.out.println("Encoded into "+encoder.getNextKey()+" categories");

        countSingles(
                encoded,
                flag
        );

        countSet(
                encoded,
                flag,
                2
        );

        countSet(
                encoded,
                flag,
                3
        );

        for (int o = 1; o <= 3; o++) {
            HashSet<IntSet> curResults = setSaved.get(o);
            HashMap<IntSet, Integer> idxMapping = setIdxMapping.get(o);
            int[] oCounts = setOCounts.get(o);
            int[] counts = setCounts.get(o);
            for (IntSet vs : curResults) {
                System.out.println("======");
                for (int v : vs.getSet()) {
                    System.out.println(encoder.decodeColumnName(v) + "=" + encoder.decodeValue(v));
                }
                int idx = idxMapping.get(vs);
                int oCount = oCounts[idx];
                int count = counts[idx];
                System.out.println("Matched Outliers / Matched Total: "+oCount+"/"+count);
                double lift = (oCount*1.0/count) / baseRate;
                System.out.println("Risk Ratio (Lift) of: "+lift);
            }
        }
    }

    private void countSet(List<int[]> encoded, boolean[] flag, int order) {
        System.out.println("Counting Order: "+order);
        long startTime = System.currentTimeMillis();
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

        for (int i = 0; i < n; i++) {
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
                counts[setIdx]++;
                if (flag[i]) {
                    oCounts[setIdx]++;
                }
            }
        }
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Counted order "+order+" in: "+elapsed);

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

        System.out.println("Saved: "+saved.size());
        System.out.println("Pruned: "+numPruned);
        System.out.println("Next: "+next.size());

        setIdxMapping.put(order, setMapping);
        setSaved.put(order, saved);
        setNext.put(order, next);
        setCounts.put(order, counts);
        setOCounts.put(order, oCounts);
    }

    private void countSingles(List<int[]> encoded, boolean[] flag) {
        // Counting Singles
        long startTime = System.currentTimeMillis();
        int[] singleCounts = new int[numSingles];
        int[] singleOCounts = new int[numSingles];
        for (int i = 0; i < n; i++) {
            int[] curRow = encoded.get(i);
            for (int v : curRow) {
                singleCounts[v]++;
            }
            if (flag[i]) {
                for (int v : curRow) {
                    singleOCounts[v]++;
                }
            }
        }
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Counted Singles in: "+elapsed);

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
        System.out.println("Saved: "+singleSaved.size());
        System.out.println("Pruned: "+numPruned);
        System.out.println("Next: "+singleNext.size());

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
        return null;
    }
}
