package edu.stanford.futuredata.macrobase.analysis.summary.aplinear;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class APLExplanation implements Explanation {

    private AttributeEncoder encoder;
    private List<String> aggregateNames;
    private long numTotal;
    private long numOutliers;

    private ArrayList<QualityMetric> metrics;
    private ArrayList<APLExplanationResult> results;

    public APLExplanation(
        AttributeEncoder encoder,
        long numTotal,
        long numOutliers,
        List<String> aggregateNames,
        List<QualityMetric> metrics,
        List<APLExplanationResult> results
    ) {
        this.encoder = encoder;
        this.numTotal = numTotal;
        this.numOutliers = numOutliers;
        this.aggregateNames = aggregateNames;
        this.metrics = new ArrayList<>(metrics);
        this.results = new ArrayList<>(results);
    }

    public List<APLExplanationResult> getResults() {
        return results;
    }

    @JsonProperty("results")
    public List<Map<String, Map<String, String>>> results() {
        List<Map<String, Map<String, String>>> r = new ArrayList<>();
        for (APLExplanationResult is : results) {
            r.add(is.jsonPrint(encoder, aggregateNames));
        }
        return r;
    }

    @JsonProperty("numTotal")
    public double numTotal() {
        return numTotal;
    }

    @JsonProperty("outliers")
    public double numOutliers() {
        return numOutliers;
    }

    @Override
    public String prettyPrint() {
        StringBuilder header = new StringBuilder(String.format(
            "Outlier Explanation:\n"
        ));
        header.append("Outliers: " + numOutliers + ", Total: " + numTotal + "\n");
        for (APLExplanationResult is : results) {
            header.append(
                "---\n" + is.prettyPrint(encoder, aggregateNames)
            );
        }
        return header.toString();
    }

    /**
     * Convert List of {@link APLExplanationResult} to normalized DataFrame that includes all
     * metrics contained in each results.
     *
     * @param attrsToInclude the attributes (String columns) to be included in the DataFrame
     * @return New DataFrame with <tt>attrsToInclude</tt> columns and ratio metric, support, and
     * outlier count columns
     */
    public DataFrame toDataFrame(final List<String> attrsToInclude) {
        // String column values that will be added to DataFrame
        final Map<String, String[]> stringResultsByCol = new HashMap<>();
        for (String colName : attrsToInclude) {
            stringResultsByCol.put(colName, new String[results.size()]);
        }

        // double column values that will be added to the DataFrame
        final Map<String, double[]> doubleResultsByCol = new HashMap<>();
        for (String colName : aggregateNames) {
            doubleResultsByCol.put(colName, new double[results.size()]);
        }
        for (QualityMetric metric : metrics) {
            // NOTE: we assume that the QualityMetrics here are the same ones
            // that each APLExplanationResult has
            doubleResultsByCol.put(metric.name(), new double[results.size()]);
        }

        // Add result rows to individual columns
        int i = 0;
        for (APLExplanationResult result : results) {
            // attrValsInRow contains the values for the explanation attribute values in this
            // given row
            final Map<String, String> attrValsInRow = result.prettyPrintMatch(encoder);
            for (String colName : stringResultsByCol.keySet()) {
                // Iterate over all attributes that will be in the DataFrame.
                // If attribute is present in attrValsInRow, add its corresponding value.
                // Otherwise, add null
                stringResultsByCol.get(colName)[i] = attrValsInRow.get(colName);
            }

            final Map<String, Double> metricValsInRow = result.getMetricsAsMap();
            for (String colName : metricValsInRow.keySet()) {
                doubleResultsByCol.get(colName)[i] = metricValsInRow.get(colName);
            }

            final Map<String, Double> aggregateValsInRow = result
                .getAggregatesAsMap(aggregateNames);
            for (String colName : aggregateNames) {
                doubleResultsByCol.get(colName)[i] = aggregateValsInRow.get(colName);
            }
            ++i;
        }

        // Generate DataFrame with results
        final DataFrame df = new DataFrame();
        for (String colName : stringResultsByCol.keySet()) {
            df.addColumn(colName, stringResultsByCol.get(colName));
        }
        // Add metrics first, then aggregates (otherwise, we'll get arbitrary orderings)
        for (QualityMetric metric : metrics) {
            df.addColumn(metric.name(), doubleResultsByCol.get(metric.name()));
        }
        for (String colName : aggregateNames) {
            // Aggregates are capitalized for some reason
            df.addColumn(colName.toLowerCase(), doubleResultsByCol.get(colName));
        }
        return df;
    }
}
