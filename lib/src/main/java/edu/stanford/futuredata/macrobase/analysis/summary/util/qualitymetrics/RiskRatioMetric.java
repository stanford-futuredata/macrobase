package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

import edu.stanford.futuredata.macrobase.analysis.summary.fpg.RiskRatio;

/**
 * Measures the relative outlier rate w.r.t. the global outlier rate
 */
public class RiskRatioMetric implements QualityMetric{
    private int outlierCountIdx;
    private int totalCountIdx;
    private double totalOutliers;
    private double totalInliers;

    public RiskRatioMetric(int outlierCountIdx, int totalCountIdx) {
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
