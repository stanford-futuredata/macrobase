package edu.stanford.futuredata.macrobase.analysis.classify.stats;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import edu.stanford.futuredata.macrobase.util.MacrobaseInternalError;

import java.util.*;

/**
 * Computes percentiles when given an array of metrics and the count of the
 * number of times each occurs. Useful for computing percentiles on cubed data.
 */
public class WeightedPercentile {
    private double[] counts;
    private double[] metrics;

    // Computed
    private int numRawMetrics = 0;
    private Multiset<Double> metricCounts;
    private List<Double> sortedMetrics;

    public WeightedPercentile(double[] counts, double[] metrics) {
        this.counts = counts;
        this.metrics = metrics;
        computeCounts();
    }

    public double evaluate(double percentile) {
        if (percentile >= 50.0) {
            int numToPass = (int)((100.0 - percentile) / 100.0 * numRawMetrics);
            int numPassed = 0;
            for (int i = sortedMetrics.size() - 1; i >= 0; i--) {
                numPassed += metricCounts.count(sortedMetrics.get(i));
                if (numPassed >= numToPass) {
                    return sortedMetrics.get(i);
                }
            }
        } else {
            int numToPass = (int)(percentile / 100.0 * numRawMetrics);
            int numPassed = 0;
            for (int i = 0; i < sortedMetrics.size(); i++) {
                numPassed += metricCounts.count(sortedMetrics.get(i));
                if (numPassed >= numToPass) {
                    return sortedMetrics.get(i);
                }
            }
        }
        throw new MacrobaseInternalError("WeightedPercentile was implemented incorrectly");
    }

    private void computeCounts() {
        metricCounts = HashMultiset.create();
        int len = counts.length;
        for (int i = 0; i < len; i++) {
            metricCounts.add(metrics[i], (int) counts[i]);
        }
        numRawMetrics = metricCounts.size();
        sortedMetrics = new ArrayList<Double>(metricCounts.elementSet());
        Collections.sort(sortedMetrics);
    }
}
