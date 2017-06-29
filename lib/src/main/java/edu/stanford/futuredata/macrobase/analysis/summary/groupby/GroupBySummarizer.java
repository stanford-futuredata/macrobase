package edu.stanford.futuredata.macrobase.analysis.summary.groupby;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.AttributeEncoder;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.*;

public class GroupBySummarizer extends BatchSummarizer {
    // Column Encoders
    private int n,d;

    @Override
    public void process(DataFrame input) throws Exception {
        n = input.getNumRows();
        d = attributes.size();

        boolean[] flag = new boolean[n];
        double[] outlierCol = input.getDoubleColumnByName(outlierColumn);
        int numOutliers = 0;
        for (int i = 0; i < n; i++) {
            flag[i] = predicate.test(outlierCol[i]);
            if (flag[i]) {
                numOutliers++;
            }
        }
        double baseRate = numOutliers*1.0/n;
        System.out.println("Base Rate of: "+baseRate);

        AttributeEncoder encoder = new AttributeEncoder();
        encoder.setColumnNames(attributes);
        List<Set<Integer>> encoded = encoder.encodeAttributes(input.getStringColsByName(attributes));

        System.out.println("Encoded into "+encoder.getNextKey()+" categories");

        int numSingles = encoder.getNextKey();
        int[] singleCounts = new int[numSingles];
        int[] singleOCounts = new int[numSingles];
        for (int i = 0; i < n; i++) {
            Set<Integer> curRow = encoded.get(i);
            for (int v : curRow) {
                singleCounts[v]++;
            }
            if (flag[i]) {
                for (int v : curRow) {
                    singleOCounts[v]++;
                }
            }
        }

        System.out.println("Counted Singles");

        int suppCount = (int) (minOutlierSupport * n);
        System.out.println("Min Support of: "+suppCount);
        System.out.println("Min RR of: "+minRiskRatio);

        Set<Integer> saved = new HashSet<>();
        Set<Integer> pruned = new HashSet<>();
        Set<Integer> next = new HashSet<>();

        for (int i = 0; i < numSingles; i++) {
            if (singleOCounts[i] < suppCount) {
                pruned.add(i);
            } else {
                double ratio = singleOCounts[i]*1.0 / (singleCounts[i] * baseRate);
                if (ratio > minRiskRatio) {
                    saved.add(i);
                } else {
                    next.add(i);
                }
            }
        }

        System.out.println("Saved: "+saved.size());
        System.out.println("Pruned: "+pruned.size());
        System.out.println("Next: "+next.size());

        for (int v : saved) {
            System.out.println(encoder.decodeColumnName(v)+"="+encoder.decodeValue(v));
            System.out.println(singleOCounts[v]+"/"+singleCounts[v]);
        }

        // Pairs
        HashMap<Set<Integer>, Integer> oCounts = new HashMap<>();
        HashMap<Set<Integer>, Integer> counts = new HashMap<>();
        for (int i = 0; i < n; i++) {
            Set<Integer> curRow = encoded.get(i);
            List<Integer> toExamine = new ArrayList<>();
            for (int v : curRow) {
                if (next.contains(v)) {
                    toExamine.add(v);
                }
            }

            int l = toExamine.size();
            for (int p1 = 0; p1 < l; p1++) {
                int p1v = toExamine.get(p1);
                for (int p2 = p1+1; p2 < l; p2++) {
                    int p2v = toExamine.get(p2);
                    Set<Integer> curPair = new HashSet<>();
                    curPair.add(p1v);
                    curPair.add(p2v);
                    if (counts.containsKey(curPair)) {
                        counts.put(curPair, counts.get(curPair)+1);
                    } else {
                        counts.put(curPair, 1);
                    }

                    if (flag[i]) {
                        if (oCounts.containsKey(curPair)) {
                            oCounts.put(curPair, oCounts.get(curPair)+1);
                        } else {
                            oCounts.put(curPair, 1);
                        }
                    }
                }
            }
        }

        Set<Set<Integer>> psaved = new HashSet<>();
        Set<Set<Integer>> ppruned = new HashSet<>();
        Set<Set<Integer>> pnext = new HashSet<>();
        for (Set<Integer> curPair : oCounts.keySet()) {
            int oCount = oCounts.get(curPair);
            int count = counts.get(curPair);
            if (oCount < suppCount) {
                ppruned.add(curPair);
            } else {
                double ratio = oCount * 1.0 / (count * baseRate);
                if (ratio > minRiskRatio) {
                    psaved.add(curPair);
                } else {
                    pnext.add(curPair);
                }
            }
        }

        System.out.println("Pair Saved: "+psaved.size());
        System.out.println("Pair Pruned: "+ppruned.size());
        System.out.println("Pair Next: "+pnext.size());

        for (Set<Integer> vs : psaved) {
            System.out.println("======");
            for (int v : vs) {
                System.out.println(encoder.decodeColumnName(v) + "=" + encoder.decodeValue(v));
            }
            System.out.println(oCounts.get(vs)+"/"+counts.get(vs));
        }
    }

    @Override
    public Explanation getResults() {
        return null;
    }
}
