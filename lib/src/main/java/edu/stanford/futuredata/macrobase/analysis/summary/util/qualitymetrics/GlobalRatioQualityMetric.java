package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

/**
 * Measures the relative outlier rate w.r.t. the global outlier rate
 */
public class GlobalRatioQualityMetric implements QualityMetric{
    private int outlierCountIdx;
    private int totalCountIdx;
    private double baseRate = 0.0;
    private double inlierWeight = 1.0;

    public GlobalRatioQualityMetric(int outlierCountIdx, int totalCountIdx, double inlierWeight) {
        this.outlierCountIdx = outlierCountIdx;
        this.totalCountIdx = totalCountIdx;
        this.inlierWeight = inlierWeight;
    }

    @Override
    public String name() {
        return "global_ratio";
    }

    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        double totalInliers = globalAggregates[totalCountIdx] - globalAggregates[outlierCountIdx];
        baseRate = globalAggregates[outlierCountIdx] / (globalAggregates[outlierCountIdx] + totalInliers * inlierWeight);
        return this;
    }

    @Override
    public double value(double[] aggregates) {
        double weightedInlierCount = inlierWeight * (aggregates[totalCountIdx] - aggregates[outlierCountIdx]);
        return (aggregates[outlierCountIdx] / (aggregates[outlierCountIdx] + weightedInlierCount)) / baseRate;
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }
}
