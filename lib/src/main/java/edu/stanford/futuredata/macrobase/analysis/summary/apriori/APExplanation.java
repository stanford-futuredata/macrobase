package edu.stanford.futuredata.macrobase.analysis.summary.apriori;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.ExplanationMetric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    private double minSupport;
    private double minRatio;
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

        this.minSupport = minSupport;
        this.minRatio = minRatio;
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
}
