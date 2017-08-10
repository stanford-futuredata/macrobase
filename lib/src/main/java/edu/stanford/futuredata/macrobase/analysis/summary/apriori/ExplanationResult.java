package edu.stanford.futuredata.macrobase.analysis.summary.apriori;


import edu.stanford.futuredata.macrobase.analysis.summary.ratios.ExplanationMetric;

import java.util.*;

public class ExplanationResult {
    private Map<String, String> matcher;
    private double matchedOutlier, matchedCount;
    private double totalOutlier, totalCount;

    private double support;

    public ExplanationResult(
            Map<String, String> matcher,
            double matchedOutlier,
            double matchedCount,
            double totalOutlier,
            double totalCount
    ) {
        this.matcher = matcher;
        this.matchedOutlier = matchedOutlier;
        this.matchedCount = matchedCount;
        this.totalOutlier = totalOutlier;
        this.totalCount = totalCount;

        this.support = matchedOutlier / totalOutlier;
    }

    public Map<String, String> getMatcher() {
        return matcher;
    }
    public double matchedOutlier() {
        return matchedOutlier;
    }
    public double matchedCount() {
        return matchedCount;
    }
    public double totalOutlier() { return totalOutlier; }
    public double totalCount() { return totalCount; }
    public double support() {
        return support;
    }

    public String prettyPrint() {
        return prettyPrint(new ArrayList<>());
    }

    /**
     * @param printableMetrics custom metrics to be included with result
     * @return component
     */
    public String prettyPrint(List<ExplanationMetric> printableMetrics) {
        StringJoiner matchJoiner = new StringJoiner(", ");
        matcher.forEach((k, v) -> matchJoiner.add(k+"="+v));

        StringJoiner metricJoiner = new StringJoiner(", ");
        printableMetrics.forEach(m -> metricJoiner.add(
                m.name()+": "+m.calc(
                        matchedOutlier, matchedCount,
                        totalOutlier, totalCount
                ))
        );

        return String.format(
                "outlier rate: %d / %d\n" +
                "support: %f\n" +
                "%s\n" +
                "matching: %s\n",
                (long)matchedOutlier, (long)matchedCount,
                support,
                metricJoiner.toString(),
                matchJoiner.toString()
        );
    }
}
