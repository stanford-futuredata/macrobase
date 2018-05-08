package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.classify.CountMeanShiftCubedClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.MeanShiftQualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.SupportQualityMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Summarizer that measures the shift of the mean of some value from the inlier
 * population to the outlier population.  Explanations return all sets of attributes with min
 * support among both inlier and outlier population where the shift of the mean of a value
 * from the inliers to the outliers passes some threshold.
 */
public class APLCountMeanShiftSummarizer  extends APLSummarizer {
    private Logger log = LoggerFactory.getLogger("APLMeanSummarizer");

    private double minMeanShift = 1.0;

    @Override
    public List<String> getAggregateNames() {
        return Arrays.asList("outlierCount", "inlierCount", "outlierMeanSum", "inlierMeanSum");
    }

    @Override
    public AggregationOp[] getAggregationOps() {
        AggregationOp[] curOps = {AggregationOp.SUM, AggregationOp.SUM, AggregationOp.SUM, AggregationOp.SUM};
        return curOps;
    }

    @Override
    public int[][] getEncoded(List<String[]> columns, DataFrame input) {
        return encoder.encodeAttributesAsArray(columns);
    }

    @Override
    public double[][] getAggregateColumns(DataFrame input) {
        double[] outlierCountColumn = input.getDoubleColumnByName(CountMeanShiftCubedClassifier.outlierCountColumnName);
        double[] inlierCountColumn = input.getDoubleColumnByName(CountMeanShiftCubedClassifier.inlierCountColumnName);
        double[] outlierMeanSumColumn = input.getDoubleColumnByName(CountMeanShiftCubedClassifier.outlierMeanSumColumnName);
        double[] inlierMeanSumColumn = input.getDoubleColumnByName(CountMeanShiftCubedClassifier.inlierMeanSumColumnName);

        double[][] aggregateColumns = new double[4][];
        aggregateColumns[0] = outlierCountColumn;
        aggregateColumns[1] = inlierCountColumn;
        aggregateColumns[2] = outlierMeanSumColumn;
        aggregateColumns[3] = inlierMeanSumColumn;

        return aggregateColumns;
    }

    @Override
    public List<QualityMetric> getQualityMetricList() {
        List<QualityMetric> qualityMetricList = new ArrayList<>();
        qualityMetricList.add(
                new SupportQualityMetric(0)
        );
        qualityMetricList.add(
                new SupportQualityMetric(1)
        );
        qualityMetricList.add(
                new MeanShiftQualityMetric(0, 1, 2, 3)
        );
        return qualityMetricList;
    }

    @Override
    public List<Double> getThresholds() {
        return Arrays.asList(minOutlierSupport, minOutlierSupport, minMeanShift);
    }

    @Override
    public double getNumberOutliers(double[][] aggregates) {
        double sum = 0;
        for (double outlierCount: aggregates[0])
            sum += outlierCount;
        return sum;
    }

    public void setMinMeanShift(double minMeanShift) {
        this.minMeanShift = minMeanShift;
    }

}