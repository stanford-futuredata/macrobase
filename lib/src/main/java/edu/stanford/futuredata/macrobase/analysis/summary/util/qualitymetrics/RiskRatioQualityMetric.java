package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

import edu.stanford.futuredata.macrobase.analysis.summary.fpg.RiskRatio;

/**
 * Calculates the risk ratio of a particular attribute.  This is the ratio of
 * the probability of being an outlier given the attribute to the probability
 * of being an outlier when without the attribute.
 */
public class RiskRatioQualityMetric implements QualityMetric{
    private int outlierCountIdx;
    private int totalCountIdx;
    private double totalOutliers;
    private double totalInliers;

    public RiskRatioQualityMetric(int outlierCountIdx, int totalCountIdx) {
        this.outlierCountIdx = outlierCountIdx;
        this.totalCountIdx = totalCountIdx;
    }

    @Override
    public String name() {
        return "risk_ratio";
    }

    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        totalOutliers = globalAggregates[outlierCountIdx];
        totalInliers = globalAggregates[totalCountIdx] - totalOutliers;
        return this;
    }

    @Override
    public double value(double[] aggregates) {
        return RiskRatio.compute(aggregates[totalCountIdx] - aggregates[outlierCountIdx],
                aggregates[outlierCountIdx],
                totalInliers,
                totalOutliers);
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }
}
