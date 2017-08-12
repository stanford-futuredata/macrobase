package edu.stanford.futuredata.macrobase.analysis.summary.ratios;

/**
 * P(exposure | outlier) / P(exposure | ~outlier)
 */
public class PrevalenceRatio extends ExplanationMetric {
    @Override
    public double calc(
            double matchedOutlier,
            double matchedTotal,
            double outlierCount,
            double totalCount) {
        if(outlierCount == 0 || matchedOutlier == 0) {
            return 0;
        }

        double inlierCount = totalCount - outlierCount;
        double matchedInlier = matchedTotal - matchedOutlier;

        if(matchedInlier == 0) {
            return Double.POSITIVE_INFINITY;
        }

        return (matchedOutlier / outlierCount) / (matchedInlier / inlierCount);
    }

    @Override
    public String name() {
        return "prevalence ratio";
    }
}
