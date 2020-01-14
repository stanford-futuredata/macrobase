package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

import java.util.List;

/**
 * Measures the relative shift of the mean of a value from the inlier to
 * the outlier population with chebyshev pruning.
 */
public class MeanShiftChebyQualityMetric implements QualityMetric {
    private int oCountIdx, iCountIdx, oSumIdx, iSumIdx, oSum2Idx, iSum2Idx;

    private int oSuppThreshIdx, iSuppThreshIdx;
    private double oSuppThreshold, iSuppThreshold;
    private double globalOCount, globalICount;

    public MeanShiftChebyQualityMetric(
            int oCountIdx,
            int iCountIdx,
            int oSumIdx,
            int iSumIdx,
            int oSum2Idx,
            int iSum2Idx
    ) {
        this.oCountIdx = oCountIdx;
        this.iCountIdx = iCountIdx;
        this.oSumIdx = oSumIdx;
        this.iSumIdx = iSumIdx;
        this.oSum2Idx = oSum2Idx;
        this.iSum2Idx = iSum2Idx;
    }

    public void setSupportThresholdIdxs(int i1, int i2) {
        oSuppThreshIdx = i1;
        iSuppThreshIdx = i2;
    }
    @Override
    public void setAPLThresholdsForOptimization(List<Double> thresholds) {
        oSuppThreshold = thresholds.get(oSuppThreshIdx);
        iSuppThreshold = thresholds.get(iSuppThreshIdx);
    }
    @Override
    public double maxSubgroupValue(double[] aggregates) {
        double oSum = aggregates[oSumIdx];
        double oSum2 = aggregates[oSum2Idx];
        double oCount = aggregates[oCountIdx];
        double iSum = aggregates[iSumIdx];
        double iSum2 = aggregates[iSum2Idx];
        double iCount = aggregates[iCountIdx];

        double oMean = oSum / oCount;
        double oStd = Math.sqrt(oSum2 / oCount - oMean*oMean);
        double oTargetFrac = oSuppThreshold * globalOCount / oCount;
        double oMeanMax = oMean + oStd * Math.sqrt(1/oTargetFrac);
        double oMeanMin = oMean - oStd * Math.sqrt(1/oTargetFrac);

        double iMean = iSum / iCount;
        double iStd = Math.sqrt(iSum2 / iCount - iMean*iMean);
        double iTargetFrac = iSuppThreshold * globalICount / iCount;
        double iMeanMax = iMean + iStd * Math.sqrt(1/iTargetFrac);
        double iMeanMin = iMean - iStd * Math.sqrt(1/iTargetFrac);
        double retVal;
        if (iMeanMax * iMeanMin <= 0) {
            retVal = Double.POSITIVE_INFINITY;
        } else if (iMeanMin > 0) {
            retVal = oMeanMax / iMeanMin;
        } else { // case where the values are negative
            retVal = oMeanMin / iMeanMax;
        }
        return retVal;
    }

    @Override
    public String name() {
        return "mean_shift_cheby";
    }

    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        globalOCount = globalAggregates[iCountIdx];
        globalICount = globalAggregates[oCountIdx];
        return this;
    }

    @Override
    public double value(double[] aggregates) {
        double oSum = aggregates[oSumIdx];
        double oCount = aggregates[oCountIdx];
        double iSum = aggregates[iSumIdx];
        double iCount = aggregates[iCountIdx];

        return (oSum/oCount)/(iSum/iCount);
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }
}