package edu.stanford.futuredata.macrobase.analysis.summary.apriori;


import java.util.Map;
import java.util.StringJoiner;

public class ExplanationResult {
    private Map<String, String> matcher;
    private double matchedOutlier, matchedCount;
    private double totalOutlier, totalCount;

    private double rateLift;
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

        this.rateLift = (matchedOutlier / matchedCount) / (totalOutlier / totalCount);
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
    public double rateLift() {
        return rateLift;
    }
    public double support() {
        return support;
    }

    public String prettyPrint() {
        StringJoiner joiner = new StringJoiner(",");
        matcher.forEach((k, v) -> joiner.add(k+"="+v));

        return String.format(
                "--\n" +
                        "outlier rate: %d / %d\n" +
                        "rate lift: %fx, support: %f\n" +
                        "matching: %s\n",
                (long)matchedOutlier, (long)matchedCount,
                rateLift, support,
                joiner.toString()
        );
    }
}
