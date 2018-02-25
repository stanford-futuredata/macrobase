package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

import msolver.ChebyshevMomentSolver2;
import msolver.MomentSolverBuilder;
import msolver.struct.MomentStruct;

/**
 * Measures how large a subgroup is relative to a global count
 */
public class EstimatedSupportMetric extends MomentOutlierMetric {
    public EstimatedSupportMetric(double quantile, int ka, int kb) {
        super(quantile, ka, kb);
    }

    @Override
    public String name() {
        return "est_support";
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
        return (1 - builder.getCDF(cutoff)) * currentCount / globalOutlierCount;
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
