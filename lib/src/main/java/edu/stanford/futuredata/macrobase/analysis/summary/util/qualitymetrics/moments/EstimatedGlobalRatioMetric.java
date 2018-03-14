package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.moments;

import msolver.MomentSolverBuilder;

/**
 * Measures the relative outlier rate w.r.t. the global outlier rate
 */
public class EstimatedGlobalRatioMetric extends MomentOutlierMetric {
    public EstimatedGlobalRatioMetric(double quantile, int ka, int kb) {
        super(quantile, ka, kb);
    }

    @Override
    public String name() {
        return "global_ratio";
    }

    @Override
    public double value(double[] aggregates) {
        if (aggregates[outlierCountIdx] > 0) {
            return (aggregates[outlierCountIdx] / aggregates[powerSumsBaseIdx]) / (1.0 - quantile);
        }
        else {
            MomentSolverBuilder builder = getBuilderFromAggregates(aggregates);
            return (1 - builder.getCDF(cutoff)) / (1.0 - quantile);
        }
    }

    public double getOutlierRateNeeded(double[] aggregates, double threshold) {
        return threshold * (1.0 - quantile);
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }
}
