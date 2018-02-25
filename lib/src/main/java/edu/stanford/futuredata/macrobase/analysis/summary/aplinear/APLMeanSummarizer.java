package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.MeanDevQualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.SupportQualityMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Summarizer that works over cube-based summarization based on mean shift.
 */
public class APLMeanSummarizer extends APLSummarizer {
    private Logger log = LoggerFactory.getLogger("APLMeanSummarizer");

    private String countColumn = null;

    private String meanColumn = "mean";
    private String stdColumn = "std";

    private double minStdDev = 3.0;

    @Override
    public List<String> getAggregateNames() {
        return Arrays.asList("count", "m1", "m2");
    }

    @Override
    public AggregationOp[] getAggregationOps() {
        AggregationOp[] curOps = {AggregationOp.SUM, AggregationOp.SUM, AggregationOp.SUM};
        return curOps;
    }

    @Override
    public int[][] getEncoded(List<String[]> columns, DataFrame input) {
        return encoder.encodeAttributesAsArray(columns);
    }

    @Override
    public double[][] getAggregateColumns(DataFrame input) {
        double[] meanCol = input.getDoubleColumnByName(meanColumn);
        double[] stdCol = input.getDoubleColumnByName(stdColumn);

        int numRows = meanCol.length;
        double[] countCol = processCountCol(input, countColumn, meanCol.length);

        double[] m1Col = new double[numRows];
        double[] m2Col = new double[numRows];
        for (int i = 0; i < meanCol.length; i++) {
            m1Col[i] = meanCol[i]*countCol[i];
            m2Col[i] = (stdCol[i]*stdCol[i] + meanCol[i]*meanCol[i])*countCol[i];
        }

        double[][] aggregateColumns = new double[3][];
        aggregateColumns[0] = countCol;
        aggregateColumns[1] = m1Col;
        aggregateColumns[2] = m2Col;

        return aggregateColumns;
    }

    @Override
    public List<QualityMetric> getQualityMetricList() {
        List<QualityMetric> qualityMetricList = new ArrayList<>();
        qualityMetricList.add(
                new SupportQualityMetric(0)
        );
        qualityMetricList.add(
                new MeanDevQualityMetric(0, 1, 2)
        );
        return qualityMetricList;
    }

    @Override
    public List<Double> getThresholds() {
        return Arrays.asList(minOutlierSupport, minStdDev);
    }

    @Override
    public double getNumberOutliers(double[][] aggregates) {
        int n = aggregates[0].length;
        int k = aggregates.length;
        QualityMetric meanDevMetric = qualityMetricList.get(1);
        double meanDevThreshold = thresholds.get(1);
        double[] curRow = new double[k];
        double outlierCount = 0;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < k; j++) {
                curRow[j] = aggregates[j][i];
            }
            if (meanDevMetric.value(curRow) > meanDevThreshold) {
                outlierCount += aggregates[0][i];
            }
        }
        return outlierCount;
    }

    public String getCountColumn() {
        return countColumn;
    }
    public void setCountColumn(String countColumn) {
        this.countColumn = countColumn;
    }

    public void setMeanColumn(String meanColumn) {
        this.meanColumn = meanColumn;
    }

    public void setStdColumn(String stdColumn) {
        this.stdColumn = stdColumn;
    }

    public void setMinStdDev(double minStdDev) {
        this.minStdDev = minStdDev;
    }

}
