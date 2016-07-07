package macrobase.analysis.summary.itemset.result;

import macrobase.ingest.result.ColumnValue;

import java.util.List;
import java.util.StringJoiner;

public class ItemsetResult {
    private double support;
    private double numRecords;
    private double ratioToInliers;
    private List<ColumnValue> items;
    public String additional = "";

    public ItemsetResult(double support,
                         double numRecords,
                         double ratioToInliers,
                         List<ColumnValue> items) {
        this.support = support;
        this.numRecords = numRecords;
        this.ratioToInliers = ratioToInliers;
        this.items = items;
    }

    public String prettyPrint() {
        StringJoiner joiner = new StringJoiner("\n");
        items.stream()
                .forEach(i -> joiner.add(String.format("\t%s: %s",
                                                       i.getColumn(),
                                                       i.getValue())));

        return String.format("support: %f\n" +
                             "records: %f\n" +
                             "ratio: %f\n" +
                             "\nColumns:\n%s\n%s\n",
                             support,
                             numRecords,
                             ratioToInliers,
                             joiner.toString(),
                             additional.length() > 0 ? "\n" + additional + "\n" : additional);
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

    public List<ColumnValue> getItems() {
        return items;
    }

    public ItemsetResult() {
        // JACKSON
    }
}