package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

/**
 * Measures how large a subgroup is relative to a global count
 */
public class SupportQualityMetric implements QualityMetric{
    private int countIdx;
    private double globalCount;
    private int fullNumOutliers;
    private double FPC = 1;  // finite population correction

    public SupportQualityMetric(int countIdx) {
        this.countIdx = countIdx;
    }

    public SupportQualityMetric(int countIdx, int fullNumOutliers) {
        this.countIdx = countIdx;
        this.fullNumOutliers = fullNumOutliers;
    }

    @Override
    public String name() {
        return "support";
    }


    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        globalCount = globalAggregates[countIdx];
        FPC = Math.sqrt((fullNumOutliers - globalCount) / (fullNumOutliers - 1));
        return this;
    }

    @Override
    public double value(double[] aggregates) {
        return aggregates[countIdx] / globalCount;
    }

    @Override
    public boolean isMonotonic() {
        return true;
    }

    @Override
    public double error(double[] aggregates) {
        double outlierRate = aggregates[countIdx] / globalCount;
        return Z * Math.sqrt(outlierRate * (1-outlierRate) / globalCount) * FPC;
    }

}
