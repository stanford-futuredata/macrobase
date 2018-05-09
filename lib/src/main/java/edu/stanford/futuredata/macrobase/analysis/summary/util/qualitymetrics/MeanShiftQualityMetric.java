package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics;

/**
 * Measures the relative shift of the mean of a value from the inlier to
 * the outlier population.
 */
public class MeanShiftQualityMetric implements QualityMetric {
    private int oCountIdx, iCountIdx, oMeanCountIdx, iMeanCountIdx;

    public MeanShiftQualityMetric(
            int oCountIdx,
            int iCountIdx,
            int oMeanCountIdx,
            int iMeanCountIdx
    ) {
        this.oCountIdx = oCountIdx;
        this.iCountIdx = iCountIdx;
        this.oMeanCountIdx = oMeanCountIdx;
        this.iMeanCountIdx = iMeanCountIdx;
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
        return (aggregates[oMeanCountIdx]/aggregates[oCountIdx]) / (aggregates[iMeanCountIdx]/aggregates[iCountIdx]);
    }

    @Override
    public boolean isMonotonic() {
        return false;
    }
}