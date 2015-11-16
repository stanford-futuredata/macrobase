package macrobase.analysis.result;

import macrobase.analysis.summary.itemset.result.ItemsetResult;

import java.util.List;

public class AnalysisResult {
    private int numOutliers;
    private int numInliers;
    private long loadTime;
    private long conversionTime;
    private long labelTime;
    private long summarizationTime;
    private List<ItemsetResult> itemSets;

    public AnalysisResult(int numOutliers,
                          int numInliers,
                          long loadTime,
                          long conversionTime,
                          long labelTime,
                          long summarizationTime,
                          List<ItemsetResult> itemSets) {
        this.numOutliers = numOutliers;
        this.numInliers = numInliers;
        this.loadTime = loadTime;
        this.conversionTime = conversionTime;
        this.labelTime = labelTime;
        this.summarizationTime = summarizationTime;
        this.itemSets = itemSets;
    }

    public int getNumOutliers() {
        return numOutliers;
    }

    public int getNumInliers() {
        return numInliers;
    }

    public long getLoadTime() {
        return loadTime;
    }

    public long getConversionTime() {
        return conversionTime;
    }

    public long getLabelTime() {
        return labelTime;
    }

    public long getSummarizationTime() {
        return summarizationTime;
    }

    public List<ItemsetResult> getItemSets() {
        return itemSets;
    }

    public AnalysisResult() {
        // JACKSON
    }
}
