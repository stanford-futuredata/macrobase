package edu.stanford.futuredata.macrobase.analysis.summary.apriori;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;

import java.util.ArrayList;
import java.util.List;

/**
 * An explanation attempts to summarize the differences between the outliers and inliers.
 * It is composed of multiple ExplanationResult results, each of which has a matcher which
 * is an atomic classifier for rows in a dataset.
 */
public class APExplanation implements Explanation {
    private ArrayList<ExplanationResult> results;
    private double numOutliers;
    private double numTotal;

    public APExplanation(
            List<ExplanationResult> results,
            double numOutliers,
            double numTotal
            ) {
        this.results = new ArrayList<>(results);
        this.numOutliers = numOutliers;
        this.numTotal = numTotal;
    }

    public void sortBySupport() {
        results.sort(
                (ExplanationResult a, ExplanationResult b) -> -Double.compare(a.support(), b.support())
        );
    }
    public ArrayList<ExplanationResult> getResults() {
        return results;
    }

    public double numOutliers() {
        return numOutliers;
    }
    public double numTotal() {
        return numTotal;
    }

    @Override
    public String prettyPrint() {
        StringBuilder header = new StringBuilder(String.format(
                "Outlier Explanation:\n"
                        + "numOutliers: %d\n"
                        + "numTotal: %d\n"
                        + "Results: \n",
                (long)numOutliers,
                (long)numTotal));
        for (ExplanationResult is : results) {
            header.append(is.prettyPrint());
        }
        return header.toString();
    }
}
