package macrobase.analysis.summary.itemset.result;

import macrobase.ingest.result.ColumnValue;

import java.util.Set;

/**
 * Created by Deepak on 2/25/16.
 */
public class IntermediateItemsetResult {
    private double support;
    private double numRecords;
    private double ratioToInliers;
    private Set<Integer> items;

    public IntermediateItemsetResult(double support,
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

    public void multiplySupport(double supportMultiplicativeFactor) {
        support *= supportMultiplicativeFactor;
    }

    public void addSupport(double supportIncrement) {
        support += supportIncrement;
    }

    public double getNumRecords() {
        return numRecords;
    }

    public double getRatioToInliers() {
        return ratioToInliers;
    }

    public void setRatioToInliers(double ratio) { ratioToInliers = ratio; }

    public Set<Integer> getItems() {
        return items;
    }

    public IntermediateItemsetResult() {
        // JACKSON
    }
}
