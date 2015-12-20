package macrobase.analysis.summary.itemset.result;

import macrobase.ingest.result.ColumnValue;

import java.util.List;

public class ItemsetResult {
    private double support;
    private int numRecords;
    private double ratioToInliers;
    private List<ColumnValue> items;

    public ItemsetResult(double support,
                         int numRecords,
                         double ratioToInliers,
                         List<ColumnValue> items) {
        this.support = support;
        this.numRecords = numRecords;
        this.ratioToInliers = ratioToInliers;
        this.items = items;
    }

    public double getSupport() {
        return support;
    }

    public int getNumRecords() {
        return numRecords;
    }

    public double getRatioToInliers() {
        return ratioToInliers;
    }

    public void setRatioToInliers(double ratio) { ratioToInliers = ratio; }

    public List<ColumnValue> getItems() {
        return items;
    }

    public ItemsetResult() {
        // JACKSON
    }
}