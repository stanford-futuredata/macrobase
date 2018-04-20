package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

/**
 * Measures the relative outlier rate w.r.t. the global outlier rate
 */
public class GlobalRatioQualityMetric implements QualityMetric{
    private int outlierCountIdx;
    private int totalCountIdx;
    private double baseRate = 0.0;
    private double inlierWeight = 1.0;
    private double outlierSampleRate;
    private double globalOutlierCount;
    private double globalInlierCount;

    public GlobalRatioQualityMetric(int outlierCountIdx, int totalCountIdx, double inlierWeight, double outlierSampleRate) {
        this.outlierCountIdx = outlierCountIdx;
        this.totalCountIdx = totalCountIdx;
        this.inlierWeight = inlierWeight;
        this.outlierSampleRate = outlierSampleRate;
    }

    @Override
    public String name() {
        return "global_ratio";
    }

    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        globalOutlierCount = globalAggregates[outlierCountIdx];
        globalInlierCount = globalAggregates[totalCountIdx] - globalAggregates[outlierCountIdx];
        baseRate = globalOutlierCount / (globalOutlierCount + globalInlierCount * inlierWeight);
        return this;
    }

    @Override
    public double value(double[] aggregates) {
//        double outlierRate = aggregates[outlierCountIdx] / globalOutlierCount;
//        double scaledInlierRate = inlierWeight * (aggregates[totalCountIdx] - aggregates[outlierCountIdx]) / globalInlierCount;
//        return outlierRate / (outlierRate + scaledInlierRate);

        double weightedInlierCount = inlierWeight * (aggregates[totalCountIdx] - aggregates[outlierCountIdx]);
        return (aggregates[outlierCountIdx] / (aggregates[outlierCountIdx] + weightedInlierCount)) / baseRate;
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }

    @Override
    public double error(double[] aggregates) {
//        double outlierRate = aggregates[outlierCountIdx] / globalOutlierCount;
//        double inlierRate = (aggregates[totalCountIdx] - aggregates[outlierCountIdx]) / globalInlierCount;
//
//        double outlierRateError = Z * Math.sqrt(outlierRate * (1-outlierRate) / globalOutlierCount);
//        double inlierRateError = Z * Math.sqrt(inlierRate * (1-inlierRate) / globalInlierCount);
//        double scaledInlierRateError = inlierWeight * inlierRateError;
//        double denomError = Math.sqrt(outlierRateError * outlierRateError + scaledInlierRateError * scaledInlierRateError);

        // TODO: fix this
        double inlierCount = aggregates[totalCountIdx] - aggregates[outlierCountIdx];
        double estimatedCount = aggregates[outlierCountIdx] + inlierWeight * inlierCount;
//        double denomError = estimatedCount * Math.sqrt(1 / aggregates[outlierCountIdx] + inlierWeight * inlierWeight / inlierCount);
        double denomError = Math.sqrt(aggregates[outlierCountIdx] + inlierWeight * inlierWeight * inlierCount);
        double g = Math.pow(Z * denomError / estimatedCount, 2);
//        System.out.format("count: %f pm %f, g: %f\n", estimatedCount / outlierSampleRate, denomError / outlierSampleRate, g);
        double rateError = aggregates[outlierCountIdx] / estimatedCount * Math.sqrt(1 / aggregates[outlierCountIdx] + denomError * denomError / (estimatedCount * estimatedCount));
        return Z * rateError / baseRate;
    }
}
