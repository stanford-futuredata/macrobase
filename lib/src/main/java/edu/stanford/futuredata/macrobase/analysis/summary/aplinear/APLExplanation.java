package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;

import java.util.ArrayList;
import java.util.List;

public class APLExplanation {
    private AttributeEncoder encoder;
    private List<String> aggregateNames;

    private ArrayList<QualityMetric> metrics;
    private List<Double> thresholds;
    private ArrayList<APLExplanationResult> results;

    public APLExplanation(
            AttributeEncoder encoder,
            List<String> aggregateNames,
            List<QualityMetric> metrics,
            List<Double> thresholds,
            List<APLExplanationResult> results
    ) {
        this.encoder = encoder;
        this.aggregateNames = aggregateNames;
        this.metrics = new ArrayList<>(metrics);
        this.thresholds = new ArrayList<>(thresholds);
        this.results = new ArrayList<>(results);
    }

    public List<APLExplanationResult> getResults() {
        return results;
    }

    String prettyPrint() {
        StringBuilder header = new StringBuilder(String.format(
                "Outlier Explanation:\n"
        ));
        for (APLExplanationResult is : results) {
            header.append(
                    "---\n"+is.prettyPrint(encoder, aggregateNames)
            );
        }
        return header.toString();
    }
}
