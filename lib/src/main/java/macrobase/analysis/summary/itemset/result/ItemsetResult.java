package macrobase.analysis.summary.itemset.result;

import macrobase.analysis.summary.itemset.ItemsetEncoder;

import java.util.*;

public class ItemsetResult {
    private double support;
    private double numRecords;
    private double ratioToInliers;
    private Map<String, String> items = new HashMap<>();

    public ItemsetResult(EncodedItemsetResult its, ItemsetEncoder encoder) {
        this.support = its.getSupport();
        this.numRecords = its.getNumRecords();
        this.ratioToInliers = its.getRatioToInliers();
        its.getItems().forEach(i -> items.put(encoder.decodeColumnName(i), encoder.decodeValue(i)));
    }

    public ItemsetResult(double support,
                         double numRecords,
                         double ratioToInliers,
                         Map<String, String> items) {
        this.support = support;
        this.numRecords = (long)numRecords;
        this.ratioToInliers = ratioToInliers;
        this.items = items;
    }

    public String prettyPrint(ItemsetEncoder encoder) {
        StringJoiner joiner = new StringJoiner("\n");
        items.forEach((k, v) -> joiner.add(k).add(v));

        return String.format("support: %f\n" +
                             "records: %d\n" +
                             "ratio: %f\n" +
                             "\nColumns:\n%s\n\n",
                             support,
                             numRecords,
                             ratioToInliers,
                             joiner.toString());
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

    public void setRatioToInliers(double ratio) {
        ratioToInliers = ratio;
    }

    public Map<String, String> getItems() {
        return items;
    }

    @Override
    public String toString() {
        return "ItemsetResult{" +
                "support=" + support +
                ", numRecords=" + numRecords +
                ", ratioToInliers=" + ratioToInliers +
                ", items=" + items +
                '}';
    }
}