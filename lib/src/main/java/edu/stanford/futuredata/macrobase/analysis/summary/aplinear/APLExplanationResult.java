package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.util.IntSet;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    /**
     * @return A Map with each metric value associated with the corresponding name of the metric
     */
    Map<String, Double> getMetricsAsMap() {
        final Map<String, Double> map = new HashMap<>();

        for (int i = 0; i < metricTypes.length; i++) {
            map.put(metricTypes[i].name(), metrics[i]);
        }
        return map;
    }

    /**
     * @param aggregateNames which aggregates to include in the Map
     * @return A Map with each aggregate value associated with the corresponding name of the
     * aggregate.
     */
    Map<String, Double> getAggregatesAsMap(final List<String> aggregateNames) {
        final Map<String, Double> map = new HashMap<>();

        for (int i = 0; i < aggregates.length; i++) {
            map.put(aggregateNames.get(i), aggregates[i]);
        }
        return map;
    }


    public double[] getXRayTuple() {
        return new double[]{aggregates[0] / aggregates[1], metrics[1]};
    }

    public String getStringedFeatureVector(AttributeEncoder encoder, List<String> colNames) {
        Set<Integer> values = matcher.getSet();
        Map<String, String> match = new HashMap<>();

        for (int k : values) {
            match.put(encoder.decodeColumnName(k), encoder.decodeValue(k));
        }
        String retString = "";
        for (String colName : colNames) {
            if (match.containsKey(colName)) {
                retString += match.get(colName) + ":";
            } else {
                retString += "a:";
            }
        }
        return retString;
    }

    Map<String, String> prettyPrintMatch(AttributeEncoder encoder) {
        Set<Integer> values = matcher.getSet();
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

    public String toString() {
        return "a=" + matcher.toString() + ":ag=" + Arrays.toString(aggregates) + ":mt=" + Arrays
            .toString(metrics);
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
