package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

/**
 * Measures how interesting a subgroup is as a function of its linear aggregates.
 * Risk ratio, support, and deviation from mean are examples.
 */
public interface QualityMetric {
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
