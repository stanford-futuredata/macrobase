package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

/**
 * q_1 / q_2
 */
public class InterventionQualityMetric implements QualityMetric{
    private int countIdx;
    private int outlierIdx;
    private double globalOutlierCount;
    private double globalCount;

    public InterventionQualityMetric(int outlierIdx, int countIdx) {
        this.countIdx = countIdx;
        this.outlierIdx = outlierIdx;
    }

    @Override
    public String name() {
        return "Intervention";
    }


    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        globalCount = globalAggregates[countIdx];
        globalOutlierCount = globalAggregates[outlierIdx];
        return this;
    }

    @Override
    public double value(double[] aggregates) {
        return  (globalOutlierCount - aggregates[outlierIdx]) / (globalCount - aggregates[countIdx]);
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }

}
