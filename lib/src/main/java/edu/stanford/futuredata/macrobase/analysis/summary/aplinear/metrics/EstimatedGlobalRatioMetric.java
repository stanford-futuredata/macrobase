package edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics;

import sketches.MomentSketch;

import java.util.Arrays;
import java.util.Collections;

/**
 * Measures the relative outlier rate w.r.t. the global outlier rate
 */
public class EstimatedGlobalRatioMetric implements QualityMetric{
    private int minIdx = 0;
    private int maxIdx = 1;
    private int momentsBaseIdx = 2;
    private double baseRate = 0.0;
    private double quantile;
    private double cutoff;
    private double globalCount;
    private double tolerance = 1e-10;

    public EstimatedGlobalRatioMetric(int minIdx, int maxIdx, int momentsBaseIdx,
                                      double quantile, double tolerance) {
        this.minIdx = minIdx;
        this.maxIdx = maxIdx;
        this.momentsBaseIdx = momentsBaseIdx;
        this.quantile = quantile;
        this.tolerance = tolerance;
    }

    @Override
    public String name() {
        return "est_global_ratio";
    }

    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        globalCount = globalAggregates[momentsBaseIdx];
        MomentSketch ms = new MomentSketch(tolerance);
        double[] powerSums = Arrays.copyOfRange(globalAggregates, momentsBaseIdx, globalAggregates.length);
        ms.setStats(powerSums, globalAggregates[minIdx], globalAggregates[maxIdx]);
        try {
            cutoff = ms.getQuantiles(Collections.singletonList(quantile))[0];
        } catch (Exception e) {
            cutoff = quantile * (globalAggregates[maxIdx] - globalAggregates[minIdx]) + globalAggregates[minIdx];
        }
        baseRate = 1.0 - quantile;
        return this;
    }

    @Override
    public double value(double[] aggregates) {
        MomentSketch ms = new MomentSketch(tolerance);
        double[] powerSums = Arrays.copyOfRange(aggregates, momentsBaseIdx, aggregates.length);
        ms.setStats(powerSums, aggregates[minIdx], aggregates[maxIdx]);
        return ms.estimateGreaterThanThreshold(cutoff) / baseRate;
    }

    @Override
    public boolean canPassThreshold(double[] aggregates, double threshold) {
        return true;
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }
}
