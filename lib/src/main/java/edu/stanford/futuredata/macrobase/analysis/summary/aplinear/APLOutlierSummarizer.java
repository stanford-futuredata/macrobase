package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.GlobalRatioMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.SupportMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Summarizer that works over both cube and row-based labeled ratio-based
 * outlier summarization.
 */
public class APLOutlierSummarizer extends APLSummarizer {
    private Logger log = LoggerFactory.getLogger("APLOutlierSummarizer");
    private String outlierColumn = "_OUTLIER";
    private String countColumn = null;
    private double minOutlierSupport = 0.1;
    private double minRatioMetric = 2.0;

    @Override
    public List<String> getAggregateNames() {
        return Arrays.asList("Outliers", "Count");
    }

    @Override
    public double[][] getAggregateColumns(DataFrame input) {
        double[] outlierCol = input.getDoubleColumnByName(outlierColumn);
        double[] countCol = processCountCol(input, countColumn,  outlierCol.length);

        double[][] aggregateColumns = new double[2][];
        aggregateColumns[0] = outlierCol;
        aggregateColumns[1] = countCol;

        return aggregateColumns;
    }

    @Override
    public List<QualityMetric> getQualityMetricList() {
        List<QualityMetric> qualityMetricList = new ArrayList<>();
        qualityMetricList.add(
                new SupportMetric(0)
        );
        qualityMetricList.add(
                new GlobalRatioMetric(0, 1)
        );
        return qualityMetricList;
    }

    @Override
    public List<Double> getThresholds() {
        return Arrays.asList(minOutlierSupport, minRatioMetric);
    }

    @Override
    public double getNumberOutliers(double[][] aggregates) {
        double count = 0.0;
        double[] outlierCount = aggregates[0];
        for (int i = 0; i < outlierCount.length; i++) {
            count += outlierCount[i];
        }
        return count;
    }

    @Override
    public APLExplanation getResults() {
        return explanation;
    }

    public String getCountColumn() {
        return countColumn;
    }
    public void setCountColumn(String countColumn) {
        this.countColumn = countColumn;
    }
    public double getMinRatioMetric() {
        return minRatioMetric;
    }
    public void setMinRatioMetric(double minRatioMetric) {
        this.minRatioMetric = minRatioMetric;
    }
    public void setMinSupport(double minSupport) {
        this.minOutlierSupport = minSupport;
    }
    public void setOutlierColumn(String outlierColumn) {
        this.outlierColumn = outlierColumn;
    }
}
