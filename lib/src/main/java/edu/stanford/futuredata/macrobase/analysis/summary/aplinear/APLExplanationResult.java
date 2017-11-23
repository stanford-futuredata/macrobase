package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.apriori.IntSet;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;

import java.util.*;

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

    private String prettyPrintMatch(AttributeEncoder encoder) {
        Set<Integer> values = matcher.getSet();

        StringJoiner matchJoiner = new StringJoiner(", ");
        for (int k : values) {
            matchJoiner.add(encoder.decodeColumnName(k)+"="+encoder.decodeValue(k));
        }
        return matchJoiner.toString();
    }

    private String prettyPrintMetric() {
        StringJoiner metricJoiner = new StringJoiner(", ");
        for (int i = 0; i < metricTypes.length; i++) {
            metricJoiner.add(
                    String.format("%s: %.3f", metricTypes[i].name(), metrics[i])
            );
        }
        return metricJoiner.toString();
    }

    private String prettyPrintAggregate(List<String> aggregateNames) {
        StringJoiner aggregateJoiner = new StringJoiner(", ");
        for (int i = 0; i < aggregates.length; i++) {
            aggregateJoiner.add(
                    String.format("%s: %.3f", aggregateNames.get(i), aggregates[i])
            );
        }
        return aggregateJoiner.toString();
    }

    @Override
    public String toString() {
        return "a="+Arrays.toString(matcher.values)+":ag="+Arrays.toString(aggregates)+":mt="+Arrays.toString(metrics);
    }

    public Map<String, String> jsonPrint(AttributeEncoder encoder,
                                  List<String> aggregateNames) {
        return new HashMap<String, String>() {{
            put("match", prettyPrintMatch(encoder));
            put("metric", prettyPrintMetric());
            put("aggregate", prettyPrintAggregate(aggregateNames));

        }};
    }

    public String prettyPrint(
            AttributeEncoder encoder,
            List<String> aggregateNames
    ) {
        return String.format(
                "%s\n%s\n%s\n",
                prettyPrintMetric(),
                prettyPrintMatch(encoder),
                prettyPrintAggregate(aggregateNames)
        );
    }
}
