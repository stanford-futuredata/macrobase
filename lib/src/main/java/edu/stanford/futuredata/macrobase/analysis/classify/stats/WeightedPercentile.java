package edu.stanford.futuredata.macrobase.analysis.classify.stats;

import edu.stanford.futuredata.macrobase.util.MacroBaseInternalError;

import java.util.*;

/**
 * Computes percentiles when given an array of metrics and the count of the
 * number of times each occurs. Useful for computing percentiles on cubed data.
 */
public class WeightedPercentile {
    private double[] counts;
    private double[] metrics;

    // Computed
    private double numRawMetrics = 0;
    private WeightedMetric[] weightedMetrics;

    public WeightedPercentile(double[] counts, double[] metrics) {
        this.counts = counts;
        this.metrics = metrics;
        computeCounts();
    }

    public double evaluate(double percentile) {
        if (percentile >= 50.0) {
            int numToPass = (int)((100.0 - percentile) / 100.0 * numRawMetrics);
            int numPassed = 0;
            for (int i = weightedMetrics.length - 1; i >= 0; i--) {
                numPassed += weightedMetrics[i].count;
                if (numPassed >= numToPass) {
                    return weightedMetrics[i].metric;
                }
            }
        } else {
            int numToPass = (int)(percentile / 100.0 * numRawMetrics);
            int numPassed = 0;
            for (int i = 0; i < weightedMetrics.length; i++) {
                numPassed += weightedMetrics[i].count;
                if (numPassed >= numToPass) {
                    return weightedMetrics[i].metric;
                }
            }
        }
        throw new MacroBaseInternalError("WeightedPercentile was implemented incorrectly");
    }

    private void computeCounts() {
        int len = metrics.length;
        weightedMetrics = new WeightedMetric[len];
        for (int i = 0; i < len; i++) {
            weightedMetrics[i] = new WeightedMetric(metrics[i], counts[i]);
            numRawMetrics += counts[i];
        }
        Arrays.sort(weightedMetrics);
    }

    public class WeightedMetric implements Comparable<WeightedMetric> {
        public double metric;
        public double count;

        public WeightedMetric(double metric, double count) {
            this.metric = metric;
            this.count = count;
        }

        @Override
        public int compareTo(WeightedMetric wm) {
            return Double.compare(metric, wm.metric);
        }
    }
}
