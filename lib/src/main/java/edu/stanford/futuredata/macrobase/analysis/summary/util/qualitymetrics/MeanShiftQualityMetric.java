package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

public class MeanShiftQualityMetric implements QualityMetric {
    private int oCountIdx, iCountIdx, oMeanIdx, iMeanIdx;

    public MeanShiftQualityMetric(
            int oCountIdx,
            int iCountIdx,
            int oMeanIdx,
            int iMeanIdx
    ) {
        this.oCountIdx = oCountIdx;
        this.iCountIdx = iCountIdx;
        this.oMeanIdx = oMeanIdx;
        this.iMeanIdx = iMeanIdx;
    }

    @Override
    public String name() {
        return "mean_shift";
    }

    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        return null;
    }

    @Override
    public double value(double[] aggregates) {
        return aggregates[oMeanIdx]/aggregates[oCountIdx] - aggregates[iMeanIdx]/aggregates[iCountIdx];
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }
}