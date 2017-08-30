package edu.stanford.futuredata.macrobase.analysis.summary.apriori;


import com.fasterxml.jackson.annotation.JsonProperty;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.ExplanationMetric;

import java.util.*;

public class ExplanationResult {
    private Map<String, String> matcher;
    private double matchedOutlier, matchedCount;
    private double totalOutlier, totalCount;

    private double support;
    private double ratio;

    public ExplanationResult(
            Map<String, String> matcher,
            double matchedOutlier,
            double matchedCount,
            double totalOutlier,
            double totalCount,
            ExplanationMetric ratioMetric
    ) {
        this.matcher = matcher;
        this.matchedOutlier = matchedOutlier;
        this.matchedCount = matchedCount;
        this.totalOutlier = totalOutlier;
        this.totalCount = totalCount;

        this.support = matchedOutlier / totalOutlier;
        this.ratio = ratioMetric.calc(matchedOutlier, matchedCount, totalOutlier, totalCount);
    }

    public Map<String, String> getMatcher() {
        return matcher;
    }

    @JsonProperty("matchedOutlier")
    public double matchedOutlier() {
        return matchedOutlier;
    }
    @JsonProperty("matchedCount")
    public double matchedCount() {
        return matchedCount;
    }
    @JsonProperty("totalOutlier")
    public double totalOutlier() { return totalOutlier; }
    @JsonProperty("totalCount")
    public double totalCount() { return totalCount; }
    @JsonProperty("support")
    public double support() {
        return support;
    }
    @JsonProperty("ratio")
    public double ratio() { return ratio; }

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
