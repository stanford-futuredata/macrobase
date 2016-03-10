package macrobase.analysis.result;

import macrobase.analysis.summary.itemset.result.ItemsetResult;

import java.util.List;
import java.util.StringJoiner;

public class AnalysisResult {
    private double numOutliers;
    private double numInliers;
    private long executionTime;
    private long loadTime;
    private long summarizationTime;
    private List<ItemsetResult> itemSets;

    public AnalysisResult(double numOutliers,
                          double numInliers,
                          long loadTime,
                          long executionTime,
                          long summarizationTime,
                          List<ItemsetResult> itemSets) {
        this.numOutliers = numOutliers;
        this.numInliers = numInliers;
        this.executionTime = executionTime;
        this.loadTime = loadTime;
        this.summarizationTime = summarizationTime;
        this.itemSets = itemSets;
    }

    @Override
    public String toString() {
        String ret = String.format("outliers: %f\n" +
                                   "inliers: %f\n" +
                                   "load time %dms\n" +
                                   "execution time: %dms\n" +
                                   "summarization time: %dms\n\n",
                                   numOutliers,
                                   numInliers,
                                   loadTime,
                                   executionTime,
                                   summarizationTime);

        final String sep = "-----\n\n";
        StringJoiner joiner = new StringJoiner(sep);
        for (ItemsetResult result : itemSets) {
            joiner.add(result.prettyPrint());
        }

        return ret + sep + joiner.toString() + sep;
    }

    public double getNumOutliers() {
        return numOutliers;
    }

    public double getNumInliers() {
        return numInliers;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public long getSummarizationTime() {
        return summarizationTime;
    }

    public long getLoadTime() {
        return loadTime;
    }

    public void setItemSets(List<ItemsetResult> itemsets) {
        this.itemSets = itemsets;
    }

    public List<ItemsetResult> getItemSets() {
        return itemSets;
    }

    public AnalysisResult() {
        // JACKSON
    }
}