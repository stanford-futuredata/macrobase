package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

public class XRayCostMetric implements QualityMetric{
    private int outlierCountIdx;
    private int totalCountIdx;
    private double alpha;

    public XRayCostMetric(int outlierCountIdx, int totalCountIdx, double alpha) {
        this.outlierCountIdx = outlierCountIdx;
        this.totalCountIdx = totalCountIdx;
        this.alpha = alpha;
    }

    @Override
    public String name() {
        return "xrayCost";
    }

    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        return this;
    }

    @Override
    public double value(double[] aggregates) {
        double errorRate = aggregates[outlierCountIdx] / aggregates[totalCountIdx];
        if (errorRate == 0)
            return 0;
        if (errorRate == 1.0)
            return 1.0;
        double fixedCost = Math.log(1/alpha)/Math.log(2);
        double falseCost = aggregates[outlierCountIdx] * Math.log(1/errorRate)/Math.log(2);
        double trueCost = (aggregates[totalCountIdx] - aggregates[outlierCountIdx]) * Math.log(1/(1-errorRate))/Math.log(2);
        return fixedCost + falseCost + trueCost;
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }
}
