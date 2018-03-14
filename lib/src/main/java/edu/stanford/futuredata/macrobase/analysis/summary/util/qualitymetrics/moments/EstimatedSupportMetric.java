package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.moments;

import msolver.MomentSolverBuilder;

/**
 * Measures how large a subgroup is relative to a global count
 */
public class EstimatedSupportMetric extends MomentOutlierMetric {
    public EstimatedSupportMetric(double quantile, int ka, int kb) {
        super(quantile, ka, kb);
    }

    @Override
    public String name() {
        return "support";
    }

    @Override
    public double value(double[] aggregates) {
        MomentSolverBuilder builder = getBuilderFromAggregates(aggregates);
        double currentCount = 0;
        if (ka > 0) {
            currentCount = aggregates[powerSumsBaseIdx];
        } else {
            currentCount = aggregates[logSumsBaseIdx];
        }
        aggregates[outlierCountIdx] = (int)((1-builder.getCDF(cutoff))*currentCount);
        return aggregates[outlierCountIdx] / globalOutlierCount;
    }

    public double getOutlierRateNeeded(double[] aggregates, double threshold) {
        if (ka > 0) {
            return threshold * globalOutlierCount / aggregates[powerSumsBaseIdx];
        } else {
            return threshold * globalOutlierCount / aggregates[logSumsBaseIdx];
        }
    }

    @Override
    public boolean isMonotonic() {
        return true;
    }
}
