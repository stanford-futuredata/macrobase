package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

/**
 * Measures how large a subgroup is relative to a global count
 */
public class InterventionQualityMetric implements QualityMetric{
    private int countIdx;
    private double globalCount;

    public InterventionQualityMetric(int countIdx) {
        this.countIdx = countIdx;
    }

    @Override
    public String name() {
        return "Intervention";
    }


    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        globalCount = globalAggregates[countIdx];
        return this;
    }

    @Override
    public double value(double[] aggregates) {
        return  globalCount - aggregates[countIdx];
    }

    @Override
    public boolean isMonotonic() {
        return true;
    }

}
