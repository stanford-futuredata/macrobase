package edu.stanford.futuredata.macrobase.analysis.summary.fpg.result;

import java.util.Set;

public class FPGItemsetResult {
    private double support;
    private double numRecords;
    private double ratioToInliers;
    private Set<Integer> items;

    public FPGItemsetResult(double support,
                            double numRecords,
                            double ratioToInliers,
                            Set<Integer> items) {
        this.support = support;
        this.numRecords = numRecords;
        this.ratioToInliers = ratioToInliers;
        this.items = items;
    }

    public double getSupport() {
        return support;
    }

    public double getNumRecords() {
        return numRecords;
    }

    public double getRatioToInliers() {
        return ratioToInliers;
    }

    public Set<Integer> getItems() {
        return items;
    }

    @Override
    public String toString() {
        return "FPGItemsetResult{" +
                "support=" + support +
                ", numRecords=" + numRecords +
                ", ratioToInliers=" + ratioToInliers +
                ", items=" + items +
                '}';
    }
}