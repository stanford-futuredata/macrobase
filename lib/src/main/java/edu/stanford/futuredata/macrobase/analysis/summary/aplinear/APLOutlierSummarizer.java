package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.*;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Summarizer that works over both cube and row-based labeled ratio-based outlier summarization.
 */
public class APLOutlierSummarizer extends APLSummarizer {

    private Logger log = LoggerFactory.getLogger("APLOutlierSummarizer");
    private String countColumn = null;
    private boolean useBitmaps;

    public APLOutlierSummarizer(boolean useBitmaps) {
        this.useBitmaps = useBitmaps;
    }

    @Override
    public List<String> getAggregateNames() {
        return Arrays.asList("Outliers", "Count");
    }

    @Override
    public AggregationOp[] getAggregationOps() {
        AggregationOp[] curOps = {AggregationOp.SUM, AggregationOp.SUM};
        return curOps;
    }

    @Override
    public int[][] getEncoded(List<String[]> columns, DataFrame input) {
        return encoder.encodeAttributesWithSupport(columns, minOutlierSupport,
            input.getDoubleColumnByName(outlierColumn), useBitmaps);
    }

    @Override
    public double[][] getAggregateColumns(DataFrame input) {
        double[] outlierCol = input.getDoubleColumnByName(outlierColumn);
        double[] countCol = processCountCol(input, countColumn, outlierCol.length);

        double[][] aggregateColumns = new double[2][];
        aggregateColumns[0] = outlierCol;
        aggregateColumns[1] = countCol;

        return aggregateColumns;
    }

    @Override
    public List<QualityMetric> getQualityMetricList() {
        List<QualityMetric> qualityMetricList = new ArrayList<>();
        qualityMetricList.add(
            new SupportQualityMetric(0)
        );
        switch (ratioMetric) {
            case "risk_ratio":
            case "riskratio":
                qualityMetricList.add(
                    new RiskRatioQualityMetric(0, 1));
                break;
            case "prevalence_ratio":
            case "prevalenceratio":
                qualityMetricList.add(
                    new PrevalenceRatioQualityMetric(0, 1));
                break;
            case "global_ratio":
            case "globalratio":
            default:
                qualityMetricList.add(
                    new GlobalRatioQualityMetric(0, 1));
        }
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

    public String getCountColumn() {
        return countColumn;
    }

    public void setCountColumn(String countColumn) {
        this.countColumn = countColumn;
    }

    public double getMinRatioMetric() {
        return minRatioMetric;
    }
}
