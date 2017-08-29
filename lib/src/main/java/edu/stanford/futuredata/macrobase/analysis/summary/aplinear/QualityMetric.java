package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

interface QualityMetric {
    String name();
    QualityMetric initialize(double[] globalAggregates);
    double value(double[] aggregates);
    boolean isMonotonic();

    // can override for more fancy tight quality metric bounds
    default double maxSubgroupValue(double[] aggregates) {
        if (isMonotonic()) {
            return value(aggregates);
        } else {
            return Double.POSITIVE_INFINITY;
        }
    }
}
