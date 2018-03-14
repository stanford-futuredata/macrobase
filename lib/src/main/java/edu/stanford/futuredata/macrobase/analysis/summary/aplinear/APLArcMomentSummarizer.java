package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.AggregationOp;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.amoments.EstimatedGlobalRatioMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.amoments.EstimatedSupportMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.amoments.MomentOutlierMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Summarizer that works over cubed data with moments.
 */
public class APLArcMomentSummarizer extends APLSummarizer {
    private Logger log = LoggerFactory.getLogger("APLArcMomentSummarizer");
    private int k;
    private String minColumn = null;
    private String maxColumn = null;
    private List<String> powerSumColumns;
    private double quantileCutoff;
    private boolean useCascade = true;
    private boolean useSupport = true;
    private boolean useGlobalRatio = true;

    @Override
    public List<String> getAggregateNames() {
        ArrayList<String> aggregateNames = new ArrayList<>();
        aggregateNames.add(getMinColumn());
        aggregateNames.add(getMaxColumn());
        aggregateNames.addAll(getPowerSumColumns());
        aggregateNames.add("Outliers");
        return aggregateNames;
    }

    public int getNumAggregates() {
        return getAggregateNames().size();
    }

    @Override
    public double[][] getAggregateColumns(DataFrame input) {
        int numAggregates = getNumAggregates();
        double[][] aggregateColumns = new double[numAggregates][];

        int curCol = 0;
        aggregateColumns[curCol++] = input.getDoubleColumnByName(minColumn);
        aggregateColumns[curCol++] = input.getDoubleColumnByName(maxColumn);
        for (int i = 0; i < powerSumColumns.size(); i++) {
            aggregateColumns[curCol++] = input.getDoubleColumnByName(powerSumColumns.get(i));
        }
        // placeholder column for outlier count
        int n = input.getNumRows();
        aggregateColumns[curCol++] = new double[n];

        processCountCol(input, powerSumColumns.get(0), aggregateColumns[0].length);
        return aggregateColumns;
    }

    @Override
    public AggregationOp[] getAggregationOps() {
        int numAggregates = getNumAggregates();
        AggregationOp[] aggregationOps = new AggregationOp[numAggregates];

        int curCol = 0;
        aggregationOps[curCol++] = AggregationOp.MIN;
        aggregationOps[curCol++] = AggregationOp.MAX;
        for (int i = 0; i < k; i++) {
            aggregationOps[curCol++] = AggregationOp.SUM;
        }
        aggregationOps[curCol++] = AggregationOp.SUM;
        return aggregationOps;
    }

    @Override
    public int[][] getEncoded(List<String[]> columns, DataFrame input) {
        return encoder.encodeAttributesAsArray(columns);
    }

    @Override
    public List<QualityMetric> getQualityMetricList() {
        List<QualityMetric> qualityMetricList = new ArrayList<>();

        if (useSupport) {
            EstimatedSupportMetric metric = new EstimatedSupportMetric(quantileCutoff, k);
            metric.setUseCascade(true);
            qualityMetricList.add(metric);
        }
        if (useGlobalRatio) {
            EstimatedGlobalRatioMetric metric = new EstimatedGlobalRatioMetric(quantileCutoff, k);
            metric.setUseCascade(true);
            qualityMetricList.add(metric);
        }

        int curCol = 0;
        int minIdx = curCol++;
        int maxIdx = curCol++;
        int powerSumsBaseIdx = curCol;
        for (QualityMetric metric : qualityMetricList) {
            ((MomentOutlierMetric)metric).setStandardIndices(minIdx, maxIdx, powerSumsBaseIdx);
        }
        curCol += k;

        int outlierCountIdx = curCol++;
        for (QualityMetric metric : qualityMetricList) {
            ((MomentOutlierMetric)metric).setOutlierCountIdx(outlierCountIdx);
        }

        return qualityMetricList;
    }

    @Override
    public List<Double> getThresholds() {
        List<Double> thresholds = new ArrayList<>();
        if (useSupport) {
            thresholds.add(minOutlierSupport);
        }
        if (useGlobalRatio) {
            thresholds.add(minRatioMetric);
        }
        return thresholds;
    }

    @Override
    public double getNumberOutliers(double[][] aggregates) {
        double count = 0.0;
        double[] counts = aggregates[2];
        for (int i = 0; i < counts.length; i++) {
            count += counts[i];
        }
        return count * (1.0-quantileCutoff);
    }

    public void setK(int ka) { this.k = ka; }
    public String getMinColumn() {
        return minColumn;
    }
    public void setMinColumn(String minColumn) {
        this.minColumn = minColumn;
    }
    public String getMaxColumn() { return maxColumn; }
    public void setMaxColumn(String maxColumn) {
        this.maxColumn = maxColumn;
    }
    public List<String> getPowerSumColumns() {
        return powerSumColumns;
    }
    public void setPowerSumColumns(List<String> powerSumColumns) {
        this.powerSumColumns = powerSumColumns;
    }
    public void setQuantileCutoff(double cutoff) {
        this.quantileCutoff = cutoff;
    }
    public double getMinRatioMetric() {
        return minRatioMetric;
    }
    public void setUseCascade(boolean useCascade) { this.useCascade = useCascade; }
    public void setUseSupport(boolean useSupport) { this.useSupport = useSupport; }
    public void setUseGlobalRatio(boolean useGlobalRatio) { this.useGlobalRatio = useGlobalRatio; }
}
