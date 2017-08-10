package edu.stanford.futuredata.macrobase.analysis.summary.ratios;

/**
 * P(outlier | exposure) / P(outlier)
 * Exponential pairwise mutual information
 * Doesn't have NaN / Infty errors in edge cases as much as risk ratio does
 */
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
