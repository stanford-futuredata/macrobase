package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

/**
 * Measures the relative outlier rate w.r.t. the global outlier rate
 */
public class GlobalRatioQualityMetric implements QualityMetric{
    private int outlierCountIdx;
    private int totalCountIdx;
    private double baseRate = 0.0;

    public GlobalRatioQualityMetric(int outlierCountIdx, int totalCountIdx) {
        this.outlierCountIdx = outlierCountIdx;
        this.totalCountIdx = totalCountIdx;
    }

    @Override
    public String name() {
        return "global_ratio";
    }

    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        baseRate = globalAggregates[outlierCountIdx] / globalAggregates[totalCountIdx];
        return this;
    }

    @Override
    public double value(double[] aggregates) {
        return (aggregates[outlierCountIdx] / aggregates[totalCountIdx]) / baseRate;
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }
}
