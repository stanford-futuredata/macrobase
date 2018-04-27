package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

public class PrevalenceRatioQualityMetric implements QualityMetric {

    private int outlierCountIdx;
    private int totalCountIdx;
    private double baseRate;

    public PrevalenceRatioQualityMetric(int outlierCountIdx, int totalCountIdx) {
        this.outlierCountIdx = outlierCountIdx;
        this.totalCountIdx = totalCountIdx;
    }

    @Override
    public String name() {
        return "prevalence_ratio";
    }

    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        final double totalCount = globalAggregates[totalCountIdx];
        double outlierCount = globalAggregates[outlierCountIdx];
        if (outlierCount == 0.0) {
            outlierCount += 1.0;
        }
        baseRate = outlierCount / (totalCount - outlierCount);
        return this;
    }

    @Override
    public double value(double[] aggregates) {
//        if(outlierCount == 0 || matchedOutlier == 0) {
//            return 0;
//        }
//
//        double inlierCount = totalCount - outlierCount;
//        double matchedInlier = matchedTotal - matchedOutlier;
//
//        if(matchedInlier == 0) {
//            matchedInlier += 1; // increment by 1 to avoid DivideByZero error
//        }
//
//        return (matchedOutlier / outlierCount) / (matchedInlier / inlierCount);

        final double matchedOutlier = aggregates[outlierCountIdx];
        final double matchedTotal = aggregates[totalCountIdx];
        return matchedOutlier / (matchedTotal - matchedOutlier) / baseRate;
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }
}
