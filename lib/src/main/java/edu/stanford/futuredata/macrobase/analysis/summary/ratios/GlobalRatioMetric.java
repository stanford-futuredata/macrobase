package edu.stanford.futuredata.macrobase.analysis.summary.ratios;

public class GlobalRatioMetric extends ExplanationMetric {
    @Override
    public double calc(
            double matchedOutlier,
            double matchedTotal,
            double outlierCount,
            double totalCount) {
        return (matchedOutlier / matchedTotal) / (outlierCount / totalCount);
    }

    @Override
    public String name() {
        return "global ratio";
    }
}
