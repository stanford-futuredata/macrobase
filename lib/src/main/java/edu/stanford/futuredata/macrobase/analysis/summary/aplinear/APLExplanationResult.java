package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.apriori.IntSet;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Subgroup which meets the quality threshold metrics.
 */
public class APLExplanationResult {
    private QualityMetric[] metricTypes;
    private IntSet matcher;
    private double[] aggregates;
    private double[] metrics;

    public APLExplanationResult(
            QualityMetric[] metricTypes,
            IntSet matcher,
            double[] aggregates,
            double[] metrics
    ) {
        this.metricTypes = metricTypes;
        this.matcher = matcher;
        this.aggregates = aggregates;
        this.metrics = metrics;
    }

    @Override
    public String toString() {
        return "a="+Arrays.toString(matcher.values)+":ag="+Arrays.toString(aggregates)+":mt="+Arrays.toString(metrics);
    }

    public String prettyPrint(
            AttributeEncoder encoder,
            List<String> aggregateNames
    ) {
        Set<Integer> values = matcher.getSet();

        StringJoiner matchJoiner = new StringJoiner(", ");
        for (int k : values) {
            matchJoiner.add(encoder.decodeColumnName(k)+"="+encoder.decodeValue(k));
        }

        StringJoiner metricJoiner = new StringJoiner(", ");
        for (int i = 0; i < metricTypes.length; i++) {
            metricJoiner.add(
                    String.format("%s: %.3f", metricTypes[i].name(), metrics[i])
            );
        }

        StringJoiner aggregateJoiner = new StringJoiner(", ");
        for (int i = 0; i < aggregates.length; i++) {
            aggregateJoiner.add(
                    String.format("%s: %.3f", aggregateNames.get(i), aggregates[i])
            );
        }

        return String.format(
                "%s\n%s\n%s\n",
                metricJoiner.toString(),
                matchJoiner.toString(),
                aggregateJoiner.toString()
        );
    }
}
