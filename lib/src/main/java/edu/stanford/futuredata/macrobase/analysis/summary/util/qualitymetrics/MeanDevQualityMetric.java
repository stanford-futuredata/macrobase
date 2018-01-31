package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

/**
 * Measures how many standard deviations a subgroup is away from the global mean
 */
public class MeanDevQualityMetric implements QualityMetric {
    private double globalMean;
    private double globalStdDev;

    private int countIdx, m1Idx, m2Idx;

    public MeanDevQualityMetric(
            int countIdx,
            int m1Idx,
            int m2Idx
    ) {
        this.countIdx = countIdx;
        this.m1Idx = m1Idx;
        this.m2Idx = m2Idx;
    }

    @Override
    public String name() {
        return "mean_deviation";
    }

    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        double n = globalAggregates[countIdx];
        globalMean = globalAggregates[m1Idx] / n;
        globalStdDev = Math.sqrt(
                globalAggregates[m2Idx]/n - globalMean*globalMean
        );
        return null;
    }

    @Override
    public double value(double[] aggregates) {
        double curMean = aggregates[m1Idx] / aggregates[countIdx];
        return Math.abs((curMean - globalMean) / globalStdDev);
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }
}
