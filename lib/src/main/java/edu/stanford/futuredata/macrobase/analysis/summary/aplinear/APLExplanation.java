package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class APLExplanation implements Explanation {
    private AttributeEncoder encoder;
    private List<String> aggregateNames;
    private long numTotal;

    private ArrayList<QualityMetric> metrics;
    private List<Double> thresholds;
    private ArrayList<APLExplanationResult> results;

    public APLExplanation(
            AttributeEncoder encoder,
            long numTotal,
            List<String> aggregateNames,
            List<QualityMetric> metrics,
            List<Double> thresholds,
            List<APLExplanationResult> results
    ) {
        this.encoder = encoder;
        this.numTotal = numTotal;
        this.aggregateNames = aggregateNames;
        this.metrics = new ArrayList<>(metrics);
        this.thresholds = new ArrayList<>(thresholds);
        this.results = new ArrayList<>(results);
    }

    public List<APLExplanationResult> getResults() {
        return results;
    }

    @JsonProperty("results")
    public List<Map<String, String>> results() {
        List<Map<String, String>> r = new ArrayList<>();
        for (APLExplanationResult is : results) {
            r.add(is.jsonPrint(encoder, aggregateNames));
        }
        return r;
    }

    @JsonProperty("numTotal")
    public double numTotal() {
        return numTotal;
    }

    @Override
    public String prettyPrint() {
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
