package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.amoments;

import msolver.PMomentSolverBuilder;

/**
 * Measures how large a subgroup is relative to a global count
 */
public class EstimatedSupportMetric extends MomentOutlierMetric {
    public EstimatedSupportMetric(double quantile, int k) {
        super(quantile, k);
    }

    @Override
    public String name() {
        return "support";
    }

    @Override
    public double value(double[] aggregates) {
        PMomentSolverBuilder builder = getBuilderFromAggregates(aggregates);
        double currentCount = aggregates[powerSumsBaseIdx];
        aggregates[outlierCountIdx] = (int)((1-builder.getCDF(cutoff))*currentCount);
        return aggregates[outlierCountIdx] / globalOutlierCount;
    }

    public double getOutlierRateNeeded(double[] aggregates, double threshold) {
        return threshold * globalOutlierCount / aggregates[powerSumsBaseIdx];
    }

    @Override
    public boolean isMonotonic() {
        return true;
    }
}
