package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSetAsLong;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;

import java.util.*;

/**
 * Subgroup which meets the quality threshold metrics.
 */
public class APLExplanationResult {
    private QualityMetric[] metricTypes;
    private long matcher;
    private double[] aggregates;
    private double[] metrics;

    public APLExplanationResult(
            QualityMetric[] metricTypes,
            long matcher,
            double[] aggregates,
            double[] metrics
    ) {
        this.metricTypes = metricTypes;
        this.matcher = matcher;
        this.aggregates = aggregates;
        this.metrics = metrics;
    }

    private Map<String, String> prettyPrintMatch(AttributeEncoder encoder) {
        Set<Integer> values = IntSetAsLong.getSet(matcher);
        Map<String, String> match = new HashMap<>();

        for (int k : values) {
            match.put(encoder.decodeColumnName(k), encoder.decodeValue(k));
        }
        return match;
    }

    private Map<String, String> prettyPrintMetric() {
        Map<String, String> metric = new HashMap<>();

        for (int i = 0; i < metricTypes.length; i++) {
            metric.put(metricTypes[i].name(), String.format("%.3f", metrics[i]));
        }
        return metric;
    }

    private Map<String, String> prettyPrintAggregate(List<String> aggregateNames) {
        Map<String, String> aggregate = new HashMap<>();

        for (int i = 0; i < aggregates.length; i++) {
            aggregate.put(aggregateNames.get(i), String.format("%.3f", aggregates[i]));
        }
        return aggregate;
    }

    public Map<String, Map<String, String>> jsonPrint(AttributeEncoder encoder,
                                  List<String> aggregateNames) {
        return new HashMap<String, Map<String, String>>() {{
            put("matcher", prettyPrintMatch(encoder));
            put("metric", prettyPrintMetric());
            put("aggregate", prettyPrintAggregate(aggregateNames));

        }};
    }

    private String removeBrackets(String str) {
        int l = str.length();
        return str.substring(1, l - 1);
    }

    public String prettyPrint(
            AttributeEncoder encoder,
            List<String> aggregateNames
    ) {
        String metricString = removeBrackets(prettyPrintMetric().toString());
        String matchString = removeBrackets(prettyPrintMatch(encoder).toString());
        String aggregateString = removeBrackets(prettyPrintAggregate(aggregateNames).toString());

        return String.format(
                "%s: %s\n%s: %s\n%s: %s\n",
                "metrics", metricString,
                "matches", matchString,
                "aggregates", aggregateString
        );
    }
}
