package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.*;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.moments.EstimatedGlobalRatioMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.moments.EstimatedSupportMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.moments.MomentOutlierMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Summarizer that works over cubed data with moments.
 */
public class APLMomentSummarizer extends APLSummarizer {
    private Logger log = LoggerFactory.getLogger("APLMomentSummarizer");
    private int ka;
    private int kb;
    private String minColumn = null;
    private String maxColumn = null;
    private String logMinColumn = null;
    private String logMaxColumn = null;
    private List<String> powerSumColumns;
    private List<String> logSumColumns;
    private double quantileCutoff;
    private boolean useCascade = true;
    private boolean useSupport = true;
    private boolean useGlobalRatio = true;

    @Override
    public List<String> getAggregateNames() {
        ArrayList<String> aggregateNames = new ArrayList<>();
        if (ka > 0) {
            aggregateNames.add(getMinColumn());
            aggregateNames.add(getMaxColumn());
            aggregateNames.addAll(getPowerSumColumns());
        }
        if (kb > 0) {
            aggregateNames.add(getLogMinColumn());
            aggregateNames.add(getLogMaxColumn());
            aggregateNames.addAll(getLogSumColumns());
        }
        aggregateNames.add("Outliers");
        return aggregateNames;
    }

    public int getNumAggregates() {
        return getAggregateNames().size();
//        int numAggregates = 0;
//        if (ka > 0) {
//            numAggregates += 2 + ka;
//        }
//        if (kb > 0) {
//            numAggregates += 2 + kb;
//        }
//        return numAggregates;
    }

    @Override
    public double[][] getAggregateColumns(DataFrame input) {
        int numAggregates = getNumAggregates();
        double[][] aggregateColumns = new double[numAggregates][];

        int curCol = 0;
        if (ka > 0) {
            aggregateColumns[curCol++] = input.getDoubleColumnByName(minColumn);
            aggregateColumns[curCol++] = input.getDoubleColumnByName(maxColumn);
            for (int i = 0; i < powerSumColumns.size(); i++) {
                aggregateColumns[curCol++] = input.getDoubleColumnByName(powerSumColumns.get(i));
            }
        }
        if (kb > 0) {
            aggregateColumns[curCol++] = input.getDoubleColumnByName(logMinColumn);
            aggregateColumns[curCol++] = input.getDoubleColumnByName(logMaxColumn);
            for (int i = 0; i < logSumColumns.size(); i++) {
                aggregateColumns[curCol++] = input.getDoubleColumnByName(logSumColumns.get(i));
            }
        }
        // placeholder column for outlier count
        int n = input.getNumRows();
        aggregateColumns[curCol++] = new double[n];

        if (ka > 0) {
            processCountCol(input, powerSumColumns.get(0), aggregateColumns[0].length);
        } else {
            processCountCol(input, logSumColumns.get(0), aggregateColumns[0].length);
        }

        return aggregateColumns;
    }

    @Override
    public AggregationOp[] getAggregationOps() {
        int numAggregates = getNumAggregates();
        AggregationOp[] aggregationOps = new AggregationOp[numAggregates];

        int curCol = 0;
        if (ka > 0) {
            aggregationOps[curCol++] = AggregationOp.MIN;
            aggregationOps[curCol++] = AggregationOp.MAX;
            for (int i = 0; i < ka; i++) {
                aggregationOps[curCol++] = AggregationOp.SUM;
            }
        }
        if (kb > 0) {
            aggregationOps[curCol++] = AggregationOp.MIN;
            aggregationOps[curCol++] = AggregationOp.MAX;
            for (int i = 0; i < kb; i++) {
                aggregationOps[curCol++] = AggregationOp.SUM;
            }
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
            EstimatedSupportMetric metric = new EstimatedSupportMetric(quantileCutoff, ka, kb);
            metric.setUseCascade(true);
            qualityMetricList.add(metric);
        }
        if (useGlobalRatio) {
            EstimatedGlobalRatioMetric metric = new EstimatedGlobalRatioMetric(quantileCutoff, ka, kb);
            metric.setUseCascade(true);
            qualityMetricList.add(metric);
        }

        int curCol = 0;
        if (ka > 0) {
            int minIdx = curCol++;
            int maxIdx = curCol++;
            int powerSumsBaseIdx = curCol;
            curCol += ka;
            for (QualityMetric metric : qualityMetricList) {
                ((MomentOutlierMetric)metric).setStandardIndices(minIdx, maxIdx, powerSumsBaseIdx);
            }
        }
        if (kb > 0) {
            int logMinIdx = curCol++;
            int logMaxIdx = curCol++;
            int logSumsBaseIdx = curCol;
            curCol += kb;
            for (QualityMetric metric : qualityMetricList) {
                ((MomentOutlierMetric)metric).setLogIndices(logMinIdx, logMaxIdx, logSumsBaseIdx);
            }
        }

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

    public void setKa(int ka) { this.ka = ka; }
    public void setKb(int kb) { this.kb = kb; }
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
    public String getLogMinColumn() { return logMinColumn;}
    public void setLogMinColumn(String logMinColumn) {
        this.logMinColumn = logMinColumn;
    }
    public String getLogMaxColumn() { return logMaxColumn;}
    public void setLogMaxColumn(String logMaxColumn) {
        this.logMaxColumn = logMaxColumn;
    }
    public List<String> getPowerSumColumns() {
        return powerSumColumns;
    }
    public void setPowerSumColumns(List<String> powerSumColumns) {
        this.powerSumColumns = powerSumColumns;
    }
    public List<String> getLogSumColumns() { return logSumColumns;}
    public void setLogSumColumns(List<String> logSumColumns) {
        this.logSumColumns = logSumColumns;
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
