package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.classify.CountMeanShiftChebyClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.CountMeanShiftClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.CountMeanShiftCubedClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.*;
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
public class APLCountMeanShiftChebySummarizer extends APLSummarizer {
    private Logger log = LoggerFactory.getLogger("APLMeanSummarizer");

    private double minMeanShift = 1.0;

    @Override
    public List<String> getAggregateNames() {
        return Arrays.asList("outlierCount", "inlierCount", "outlierMeanSum", "inlierMeanSum", "outlierSum2", "inlierSum2");
    }

    @Override
    public AggregationOp[] getAggregationOps() {
        AggregationOp[] curOps = {AggregationOp.SUM, AggregationOp.SUM,
                AggregationOp.SUM, AggregationOp.SUM, AggregationOp.SUM, AggregationOp.SUM};
        return curOps;
    }

    @Override
    public int[][] getEncoded(List<String[]> columns, DataFrame input) {
        return encoder.encodeAttributesAsArray(columns);
    }

    @Override
    public double[][] getAggregateColumns(DataFrame input) {
        double[] outlierCountColumn = input.getDoubleColumnByName(CountMeanShiftClassifier.outlierCountColumnName);
        double[] inlierCountColumn = input.getDoubleColumnByName(CountMeanShiftClassifier.inlierCountColumnName);
        double[] outlierMeanSumColumn = input.getDoubleColumnByName(CountMeanShiftClassifier.outlierMeanSumColumnName);
        double[] inlierMeanSumColumn = input.getDoubleColumnByName(CountMeanShiftClassifier.inlierMeanSumColumnName);
        double[] outlierSum2Column = input.getDoubleColumnByName(CountMeanShiftChebyClassifier.outlierSum2ColumnName);
        double[] inlierSum2Column = input.getDoubleColumnByName(CountMeanShiftChebyClassifier.inlierSum2ColumnName);

        double[][] aggregateColumns = new double[6][];
        aggregateColumns[0] = outlierCountColumn;
        aggregateColumns[1] = inlierCountColumn;
        aggregateColumns[2] = outlierMeanSumColumn;
        aggregateColumns[3] = inlierMeanSumColumn;
        aggregateColumns[4] = outlierSum2Column;
        aggregateColumns[5] = inlierSum2Column;

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
        MeanShiftChebyQualityMetric ms = new MeanShiftChebyQualityMetric(
                0, 1, 2, 3, 4, 5);
        ms.setSupportThresholdIdxs(0, 1);
        ms.setAPLThresholdsForOptimization(getThresholds());
        qualityMetricList.add(ms);
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