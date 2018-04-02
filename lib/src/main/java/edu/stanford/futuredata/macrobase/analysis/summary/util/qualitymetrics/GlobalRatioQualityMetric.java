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

    @Override
    public double error(double[] aggregates) {
        // TODO: this is wrong
        double inlierCount = aggregates[totalCountIdx] - aggregates[outlierCountIdx];
        double estimatedCount = aggregates[outlierCountIdx] + inlierWeight * inlierCount;
//        double denomError = estimatedCount * Math.sqrt(1 / aggregates[outlierCountIdx] + inlierWeight * inlierWeight / inlierCount);
        double denomError = Math.sqrt(aggregates[outlierCountIdx] + inlierWeight * inlierWeight * inlierCount);
        double g = Math.pow(Z * denomError / estimatedCount, 2);
        System.out.format("count: %f pm %f, g: %f\n", estimatedCount, denomError, g);
        double rateError = aggregates[outlierCountIdx] / estimatedCount * Math.sqrt(1 / aggregates[outlierCountIdx] + denomError * denomError / (estimatedCount * estimatedCount));
        return Z * rateError / baseRate;
    }
}
