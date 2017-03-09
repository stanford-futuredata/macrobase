package macrobase.analysis.summary.itemset.result;

import macrobase.analysis.summary.itemset.ItemsetEncoder;

import java.util.List;
import java.util.StringJoiner;

public class ItemsetResult {
    private double support;
    private double numRecords;
    private double ratioToInliers;
    private List<String> items;

    public ItemsetResult(double support,
                         double numRecords,
                         double ratioToInliers,
                         List<String> items) {
        this.support = support;
        this.numRecords = numRecords;
        this.ratioToInliers = ratioToInliers;
        this.items = items;
    }

    public String prettyPrint(ItemsetEncoder encoder) {
        StringJoiner joiner = new StringJoiner("\n");
        items.stream()
                .forEach(i -> joiner.add(i));

        return String.format("support: %f\n" +
                             "records: %f\n" +
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

    public List<String> getItems() {
        return items;
    }

    public ItemsetResult() {
        // JACKSON
    }
}