package edu.stanford.futuredata.macrobase.analysis.summary.ratios;

/**
 * Calculate generic metrics to quantify the severity of a classification result.
 * Can be extended in the future to also return confidence intervals.
 */
public abstract class ExplanationMetric {
    public abstract double calc(
            double matchedOutlier,
            double matchedTotal,
            double outlierCount,
            double totalCount
    );

    /**
     * @return name of metric, for displaying configurable results
     */
    public String name() {
        return this.getClass().toString();
    }
}
