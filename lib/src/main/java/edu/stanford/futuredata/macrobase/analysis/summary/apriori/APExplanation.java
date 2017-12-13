package edu.stanford.futuredata.macrobase.analysis.summary.apriori;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.ExplanationMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * An explanation attempts to summarize the differences between the outliers and inliers.
 * It is composed of multiple ExplanationResult results, each of which has a matcher which
 * is an atomic classifier for rows in a dataset.
 *
 * This class also keeps track of some of the parameters that went into calculating the
 * explanation, including which algorithm was used for computing the severity metrics.
 */
public class APExplanation implements Explanation {
    private ArrayList<ExplanationResult> results;
    private double numOutliers;
    private double numTotal;

    private ExplanationMetric ratioMetric;

    public APExplanation(
            List<ExplanationResult> results,
            double numOutliers,
            double numTotal,
            double minSupport,
            double minRatio,
            ExplanationMetric ratioMetric
            ) {
        this.results = new ArrayList<>(results);
        this.numOutliers = numOutliers;
        this.numTotal = numTotal;
        this.ratioMetric = ratioMetric;
    }

    public void sortBySupport() {
        results.sort(
                (ExplanationResult a, ExplanationResult b) -> -Double.compare(a.support(), b.support())
        );
    }

    public ArrayList<ExplanationResult> getResults() {
        return results;
    }

    @JsonProperty("outliers")
    public double numOutliers() {
        return numOutliers;
    }

    @JsonProperty("numTotal")
    public double numTotal() {
        return numTotal;
    }

    /**
     * When printing results, need to be aware of what custom ratio metric was used.
     * @return user readable summary of results
     */
    @Override
    public String prettyPrint() {
        StringBuilder header = new StringBuilder(String.format(
                "Outlier Explanation:\n"
                        + "numOutliers: %d\n"
                        + "numTotal: %d\n"
                        + "Results: \n",
                (long)numOutliers,
                (long)numTotal
        ));
        for (ExplanationResult is : results) {
            header.append(
                    "---\n"+
                    is.prettyPrint(Collections.singletonList(ratioMetric))
            );
        }
        return header.toString();
    }

    /**
     * Convert List of <tt>ExplanationResults</tt> to normalized DataFrame
     * @param attrsToInclude the attributes (String columns) to be included in the DataFrame
     * @return New DataFrame with <tt>attrsToInclude</tt> columns and ratio metric, support, and
     * outlier count columns
     */
    public DataFrame toDataFrame(final List<String> attrsToInclude) {
        // double column values that will be added to the DataFrame
        final List<Double> ratios = new LinkedList<>();
        final List<Double> supports = new LinkedList<>();
        final List<Double> matchedOutlierCounts = new LinkedList<>();

        // String column values that will be added to df
        final Map<String, List<String>> resultsByCol = new HashMap<>();
        for (String attr : attrsToInclude) {
            resultsByCol.put(attr, new LinkedList<>());
        }

        // Add result rows to individual columns
        for (ExplanationResult result : results) {
            final Map<String, String> singleRow = result.getMatcher();
            // singleRow contains attribute-value combinations
            for (String attr : resultsByCol.keySet()) {
                // Iterate over all attributes that will be in the DataFrame.
                // If attribute is present in singleRow, add its corresponding value.
                // Otherwise, add null
                resultsByCol.get(attr).add(singleRow.get(attr));
            }
            matchedOutlierCounts.add(result.matchedOutlier());
            ratios.add(result.ratio());
            supports.add(result.support());
        }

        // Generate DataFrame with results
        final DataFrame df = new DataFrame();
        for (String attr : resultsByCol.keySet()) {
            final List<String> attrResultVals = resultsByCol.get(attr);
            df.addColumn(attr, attrResultVals.toArray(new String[0]));
        }
        df.addColumn(ratioMetric.name(), ratios.stream().mapToDouble(x -> x).toArray());
        df.addColumn("support", supports.stream().mapToDouble(x -> x).toArray());
        df.addColumn("outlier_count",
            matchedOutlierCounts.stream().mapToDouble(x -> x).toArray());
        return df;
    }
}
