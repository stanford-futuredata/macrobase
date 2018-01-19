package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

/**
 * Measures how large a subgroup is relative to a global count
 */
public class SupportMetric implements QualityMetric{
    private int countIdx;
    private double globalCount;

    public SupportMetric(int countIdx) {
        this.countIdx = countIdx;
    }

    @Override
    public String name() {
        return "support";
    }


    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        globalCount = globalAggregates[countIdx];
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

}
