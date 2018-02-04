package edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics;

import sketches.MomentSketch;

import java.util.Arrays;
import java.util.Collections;

/**
 * Measures the relative outlier rate w.r.t. the global outlier rate
 */
public class EstimatedSupportMetric implements QualityMetric{
    private int minIdx = 0;
    private int maxIdx = 1;
    private int momentsBaseIdx = 2;
    private double quantile;  // eg, 0.99
    private double cutoff;
    private double globalCount;
    private double tolerance = 1e-10;
    private boolean useCascade = true;

    // Statistics
    public int numEnterCascade = 0;
    public int numAfterNaiveCheck = 0;
    public int numAfterMarkovBound = 0;
    public int numAfterMomentBound = 0;

    public EstimatedSupportMetric(int minIdx, int maxIdx, int momentsBaseIdx, double quantile,
                                  double tolerance, boolean useCascade) {
        this.minIdx = minIdx;
        this.maxIdx = maxIdx;
        this.momentsBaseIdx = momentsBaseIdx;
        this.quantile = quantile;
        this.tolerance = tolerance;
        this.useCascade = useCascade;
    }

    @Override
    public String name() {
        return "est_support";
    }

    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        globalCount = globalAggregates[momentsBaseIdx] * (1.0 - quantile);
        MomentSketch ms = new MomentSketch(tolerance);
        double[] powerSums = Arrays.copyOfRange(globalAggregates, momentsBaseIdx, globalAggregates.length);
        ms.setStats(powerSums, globalAggregates[0], globalAggregates[1]);
        try {
            cutoff = ms.getQuantiles(Collections.singletonList(quantile))[0];
        } catch (Exception e) {
            cutoff = quantile * (globalAggregates[maxIdx] - globalAggregates[minIdx]) + globalAggregates[minIdx];
        }
        return this;
    }

    @Override
    public double value(double[] aggregates) {
        MomentSketch ms = new MomentSketch(tolerance);
        double[] powerSums = Arrays.copyOfRange(aggregates, momentsBaseIdx, aggregates.length);
        ms.setStats(powerSums, aggregates[minIdx], aggregates[maxIdx]);
        return ms.estimateGreaterThanThreshold(cutoff) * aggregates[momentsBaseIdx] / globalCount;
    }

    @Override
    public Action getAction(double[] aggregates, double threshold) {
        if (useCascade) {
            return getActionCascade(aggregates, threshold);
        } else {
            return getActionMaxent(aggregates, threshold);
        }
    }

    private Action getActionCascade(double[] aggregates, double threshold) {
        numEnterCascade++;

        // Simple checks on min and max
        if (aggregates[maxIdx] < cutoff) {
            return Action.PRUNE;
        }
        if (aggregates[minIdx] >= cutoff) {
            return Action.KEEP;
        }
        numAfterNaiveCheck++;

        // Markov bounds
        double outlierRateNeeded = threshold * globalCount / aggregates[momentsBaseIdx];
        double mean = aggregates[momentsBaseIdx+1] / aggregates[momentsBaseIdx];
        double min = aggregates[minIdx];
        double max = aggregates[maxIdx];
        double cutoffLowerBound = Math.max(0.0, 1 - (mean - min) / (cutoff - min));
        double cutoffUpperBound = Math.min(1.0, (max - mean) / (max - cutoff));
        double outlierRateUpperBound = 1.0 - cutoffLowerBound;
        double outlierRateLowerBound = 1.0 - cutoffUpperBound;
        if (outlierRateUpperBound < outlierRateNeeded) {
            return Action.PRUNE;
        }
        if (outlierRateLowerBound >= outlierRateNeeded) {
            return Action.KEEP;
        }
        numAfterMarkovBound++;

        // Moments-based bounds
        MomentSketch ms = new MomentSketch(tolerance);
        double[] powerSums = Arrays.copyOfRange(aggregates, momentsBaseIdx, aggregates.length);
        ms.setStats(powerSums, aggregates[0], aggregates[1]);
        double[] bounds = ms.boundGreaterThanThreshold(cutoff);
        if (bounds[1] < outlierRateNeeded) {
            return Action.PRUNE;
        }
        if (bounds[0] >= outlierRateNeeded) {
            return Action.KEEP;
        }
        numAfterMomentBound++;

        // Maxent estimate
        double outlierRateEstimate = ms.estimateGreaterThanThreshold(cutoff);
        return (outlierRateEstimate >= outlierRateNeeded) ? Action.KEEP : Action.PRUNE;
    }

    private Action getActionMaxent(double[] aggregates, double threshold) {
        double outlierRateNeeded = threshold * globalCount / aggregates[momentsBaseIdx];
        MomentSketch ms = new MomentSketch(tolerance);
        double[] powerSums = Arrays.copyOfRange(aggregates, momentsBaseIdx, aggregates.length);
        ms.setStats(powerSums, aggregates[minIdx], aggregates[maxIdx]);
        double outlierRateEstimate = ms.estimateGreaterThanThreshold(cutoff);
        return (outlierRateEstimate >= outlierRateNeeded) ? Action.KEEP : Action.PRUNE;
    }

    @Override
    public boolean isMonotonic() {
        return true;
    }
}
