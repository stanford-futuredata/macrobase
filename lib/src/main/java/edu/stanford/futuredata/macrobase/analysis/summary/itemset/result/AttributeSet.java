package edu.stanford.futuredata.macrobase.analysis.summary.itemset.result;

import edu.stanford.futuredata.macrobase.analysis.summary.itemset.AttributeEncoder;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class AttributeSet implements Comparable<AttributeSet>{
    private double support;
    private long numRecords;
    private double ratioToInliers;
    private Map<String, String> items = new HashMap<>();

    public AttributeSet(ItemsetResult its, AttributeEncoder encoder) {
        this.support = its.getSupport();
        this.numRecords = (long)its.getNumRecords();
        this.ratioToInliers = its.getRatioToInliers();
        its.getItems().forEach(i -> items.put(encoder.decodeColumnName(i), encoder.decodeValue(i)));
    }

    public AttributeSet(double support,
                        double numRecords,
                        double ratioToInliers,
                        Map<String, String> items) {
        this.support = support;
        this.numRecords = (long)numRecords;
        this.ratioToInliers = ratioToInliers;
        this.items = items;
    }

    public String prettyPrint() {
        StringJoiner joiner = new StringJoiner("\n");
        items.forEach((k, v) -> joiner.add(k+"="+v));

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
        return "AttributeSet{" +
                "support=" + support +
                ", numRecords=" + numRecords +
                ", ratioToInliers=" + ratioToInliers +
                ", items=" + items +
                '}';
    }

    @Override
    public int compareTo(AttributeSet o) {
        double r1 = this.getRatioToInliers();
        double r2 = o.getRatioToInliers();
        return Double.compare(r1, r2);
    }
}