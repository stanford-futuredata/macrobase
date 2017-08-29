package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

public class SupportMetric implements QualityMetric{
    private int outlierCountIdx;
    private double globalOutliercount;

    public SupportMetric(int outlierCountIdx) {
        this.outlierCountIdx = outlierCountIdx;
    }

    @Override
    public String name() {
        return "support";
    }


    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        globalOutliercount = globalAggregates[outlierCountIdx];
        return this;
    }

    @Override
    public double value(double[] aggregates) {
        return aggregates[outlierCountIdx] / globalOutliercount;
    }

    @Override
    public boolean isMonotonic() {
        return true;
    }

}
