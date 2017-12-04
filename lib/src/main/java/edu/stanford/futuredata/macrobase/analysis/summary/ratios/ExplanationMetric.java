package edu.stanford.futuredata.macrobase.analysis.summary.ratios;

import edu.stanford.futuredata.macrobase.util.MacrobaseException;

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

    /**
     * @param metricName a String that maps to a particular subclass of ExplanationMetric
     * @return an instantiation of the subclass. "risk_ratio/riskratio" -> {@link RiskRatioMetric},
     * "global_ratio/globalratio" -> {@link GlobalRatioMetric}
     */
    public static ExplanationMetric getMetricFn(final String metricName) throws MacrobaseException {
        switch (metricName.toLowerCase()) {
            case "risk_ratio":
            case "riskratio":
              return new RiskRatioMetric();
            case "global_ratio":
            case "globalratio":
              return new GlobalRatioMetric();
            default:
                throw new MacrobaseException(metricName + " is not a valid ExplanationMetric");

        }
    }
}
