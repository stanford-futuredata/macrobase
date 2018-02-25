package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

import msolver.MomentSolverBuilder;
import msolver.struct.MomentStruct;

import java.util.Arrays;

/**
 * Quality metric used in the power cube pipeline. Uses min, max and moments.
 */
public abstract class MomentOutlierMetric implements QualityMetric {
    int ka;
    int kb;
    private int minIdx;
    private int maxIdx;
    private int logMinIdx;
    private int logMaxIdx;
    int powerSumsBaseIdx;
    int logSumsBaseIdx;

    double quantile;  // eg, 0.99
    double cutoff;
    double globalOutlierCount;

    private double tolerance = 1e-9;
    private boolean useCascade = true;

    MomentOutlierMetric(double quantile, int ka, int kb) {
        this.quantile = quantile;
        this.ka = ka;
        this.kb = kb;
    }

    protected MomentSolverBuilder getBuilderFromAggregates(double[] aggregates) {
        MomentSolverBuilder b = new MomentSolverBuilder(momentDataFromAggregates(aggregates));
        b.initialize();
        return b;
    }
    private MomentStruct momentDataFromAggregates(double[] aggregates) {
        MomentStruct m = new MomentStruct();
        m.min = 0;
        m.max = 1;
        m.logMin = 0;
        m.logMax = 1;
        m.powerSums = new double[]{1};
        m.logSums = new double[]{1};

        if (ka > 0) {
            m.min = aggregates[minIdx];
            m.max = aggregates[maxIdx];
            m.powerSums = Arrays.copyOfRange(aggregates, powerSumsBaseIdx, powerSumsBaseIdx + ka);
        }
        if (kb > 0) {
            m.logMin = aggregates[logMinIdx];
            m.logMax = aggregates[logMaxIdx];
            m.logSums = Arrays.copyOfRange(aggregates, logSumsBaseIdx, logSumsBaseIdx + kb);
        }
        return m;
    }
    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        if (ka > 0) {
            globalOutlierCount = globalAggregates[powerSumsBaseIdx] * (1.0 - quantile);
        } else {
            globalOutlierCount = globalAggregates[logSumsBaseIdx] * (1.0 - quantile);
        }

        MomentStruct ms = momentDataFromAggregates(globalAggregates);
        MomentSolverBuilder builder = new MomentSolverBuilder(ms);
        try {
            double[] ps = new double[1];
            ps[0] = quantile;
            double[] qs = builder.getQuantiles(ps);
            cutoff = qs[0];
        } catch (Exception e) {
            if (ka > 0) {
                cutoff = quantile * (globalAggregates[maxIdx] - globalAggregates[minIdx]) + globalAggregates[minIdx];
            } else {
                cutoff = quantile * (Math.exp(globalAggregates[logMaxIdx]) - Math.exp(globalAggregates[logMinIdx])) +
                        Math.exp(globalAggregates[minIdx]);
            }
        }
        return this;
    }

    abstract double getOutlierRateNeeded(double[] aggregates, double threshold);

    private Action actionIfBelowThreshold() {
        if (isMonotonic()) {
            return Action.PRUNE;
        } else {
            return Action.NEXT;
        }
    }

    @Override
    public Action getAction(double[] aggregates, double threshold) {
        double outlierRateNeeded = getOutlierRateNeeded(aggregates, threshold);
        MomentSolverBuilder builder = getBuilderFromAggregates(aggregates);
        boolean aboveThreshold = builder.checkThreshold(cutoff, outlierRateNeeded);
        if (aboveThreshold) {
            return Action.KEEP;
        } else {
            return actionIfBelowThreshold();
        }
    }

    public void setUseCascade(boolean useCascade) { this.useCascade = useCascade; }
    public void setTolerance(double tolerance) { this.tolerance = tolerance; }
    
    public void setStandardIndices(int minIdx, int maxIdx, int powerSumsBaseIdx) {
        this.minIdx = minIdx;
        this.maxIdx = maxIdx;
        this.powerSumsBaseIdx = powerSumsBaseIdx;
    }
    public void setLogIndices(int logMinIdx, int logMaxIdx, int logSumsBaseIdx) {
        this.logMinIdx = logMinIdx;
        this.logMaxIdx = logMaxIdx;
        this.logSumsBaseIdx = logSumsBaseIdx;
    }
}
