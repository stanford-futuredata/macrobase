package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.*;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.IntStream;

/**
 * Summarizer that works over both cube and row-based labeled ratio-based
 * outlier summarization.
 */
public class APLMomentSummarizer extends APLSummarizer {
    private Logger log = LoggerFactory.getLogger("APLMomentSummarizer");
    private String minColumn = null;
    private String maxColumn = null;
    private List<String> momentColumns;
    private double percentile;
    private boolean useCascade = false;

    @Override
    public List<String> getAggregateNames() {
        ArrayList<String> aggregateNames = new ArrayList<>();
        aggregateNames.add("Minimum");
        aggregateNames.add("Maximum");
        for (int i = 0; i < momentColumns.size(); i++) {
            aggregateNames.add("M" + i);
        }
        return aggregateNames;
    }

    @Override
    public double[][] getAggregateColumns(DataFrame input) {
        double[][] aggregateColumns = new double[2+momentColumns.size()][];
        aggregateColumns[0] = input.getDoubleColumnByName(minColumn);
        aggregateColumns[1] = input.getDoubleColumnByName(maxColumn);
        for (int i = 0; i < momentColumns.size(); i++) {
            aggregateColumns[i+2] = input.getDoubleColumnByName(momentColumns.get(i));
        }

        processCountCol(input, momentColumns.get(0), aggregateColumns[2].length);
        return aggregateColumns;
    }

    @Override
    public Map<String, int[]> getAggregationOps() {
        Map<String, int[]> aggregationOps = new HashMap<>();
        aggregationOps.put("add", IntStream.range(2, 2+momentColumns.size()).toArray());
        aggregationOps.put("min", new int[]{0});
        aggregationOps.put("max", new int[]{1});
        return aggregationOps;
    }

    @Override
    public List<QualityMetric> getQualityMetricList() {
        List<QualityMetric> qualityMetricList = new ArrayList<>();
        if (useCascade) {
            qualityMetricList.add(
                    new EstimatedSupportMetric(0, 1, 2,
                            (100.0 - percentile) / 100.0, 1e-5, true)
            );
        } else {
            qualityMetricList.add(
                    new EstimatedSupportMetric(0, 1, 2,
                            (100.0 - percentile) / 100.0, 1e-5, false)
            );
        }
        return qualityMetricList;
    }

    @Override
    public List<Double> getThresholds() {
        return Arrays.asList(minOutlierSupport);
    }

    @Override
    public double getNumberOutliers(double[][] aggregates) {
        double count = 0.0;
        double[] counts = aggregates[2];
        for (int i = 0; i < counts.length; i++) {
            count += counts[i];
        }
        return count * percentile / 100.0;
    }

    public String getMinColumn() {
        return minColumn;
    }
    public void setMinColumn(String minColumn) {
        this.minColumn = minColumn;
    }
    public String getMaxColumn() {
        return maxColumn;
    }
    public void setMaxColumn(String maxColumn) {
        this.maxColumn = maxColumn;
    }
    public List<String> getMomentColumns() {
        return momentColumns;
    }
    public void setMomentColumns(List<String> momentColumns) {
        this.momentColumns = momentColumns;
    }
    public void setPercentile(double percentile) {
        this.percentile = percentile;
    }
    public void setCascade(boolean useCascade) { this.useCascade = useCascade; }
    public double getMinRatioMetric() {
        return minRatioMetric;
    }
}
