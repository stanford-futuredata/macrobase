package edu.stanford.futuredata.macrobase.analysis.summary.ratios;

public class RiskRatioMetric extends ExplanationMetric {
    @Override
    public double calc(
            double matchedOutlier,
            double matchedTotal,
            double outlierCount,
            double totalCount) {
        double unMatchedOutlier = outlierCount - matchedOutlier;
        double unMatchedTotal = totalCount - matchedTotal;

        if (matchedTotal == 0 || unMatchedTotal == 0) {
            return 0;
        }
        // all outliers had this pattern
        if (unMatchedOutlier == 0) {
            return Double.POSITIVE_INFINITY;
        }

        return (matchedOutlier / matchedTotal) /
                (unMatchedOutlier / unMatchedTotal);
    }

    @Override
    public String name() {
        return "risk ratio";
    }
}
