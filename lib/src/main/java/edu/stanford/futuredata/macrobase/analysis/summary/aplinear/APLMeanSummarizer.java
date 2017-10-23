package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.GlobalRatioMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.MeanDevMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.SupportMetric;
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

    private double minSupport = 0.1;
    private double minStdDev = 3.0;

    @Override
    public List<String> getAggregateNames() {
        return Arrays.asList("count", "m1", "m2");
    }

    @Override
    public double[][] getAggregateColumns(DataFrame input) {
        double[] meanCol = input.getDoubleColumnByName(meanColumn);
        double[] stdCol = input.getDoubleColumnByName(stdColumn);

        int numRows = meanCol.length;
        double[] countCol = null;
        if (countColumn != null) {
            countCol = input.getDoubleColumnByName(countColumn);
        } else {
            countCol = new double[numRows];
            for (int i = 0; i < numRows; i++) {
                countCol[i] = 1.0;
            }
        }

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
                new SupportMetric(0)
        );
        qualityMetricList.add(
                new MeanDevMetric(0, 1, 2)
        );
        return qualityMetricList;
    }

    @Override
    public List<Double> getThresholds() {
        return Arrays.asList(minSupport, minStdDev);
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

    public void setMeanColumn(String meanColumn) {
        this.meanColumn = meanColumn;
    }

    public void setStdColumn(String stdColumn) {
        this.stdColumn = stdColumn;
    }

    public void setMinSupport(double minSupport) {
        this.minSupport = minSupport;
    }

    public void setMinStdDev(double minStdDev) {
        this.minStdDev = minStdDev;
    }

}
