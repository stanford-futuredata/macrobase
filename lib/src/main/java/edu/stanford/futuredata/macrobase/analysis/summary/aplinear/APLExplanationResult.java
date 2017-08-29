package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.apriori.IntSet;

import java.util.Arrays;

public class APLExplanationResult {
    private IntSet matcher;
    private double[] aggregates;
    private double[] metrics;

    public APLExplanationResult(
            IntSet matcher,
            double[] aggregates,
            double[] metrics
    ) {
        this.matcher = matcher;
        this.aggregates = aggregates;
        this.metrics = metrics;
    }

    @Override
    public String toString() {
        return "a="+Arrays.toString(matcher.values)+":ag="+Arrays.toString(aggregates)+":mt="+Arrays.toString(metrics);
    }
}
