package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGAttributeSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a summarization result, which contains a list of attribute values
 * and other statistics about the underlying process, e.g. num of tuples observed
 * so far.
 */
public class FPGExplanation implements Explanation {
    private final long numOutliers;
    private final long numInliers;
    private List<FPGAttributeSet> itemsets;
    private final long creationTimeMs;

    public FPGExplanation(List<FPGAttributeSet> resultList,
                          long numInliers,
                          long numOutliers,
                          long creationTimeMs) {
        itemsets = new ArrayList<>(resultList);
        this.numInliers = numInliers;
        this.numOutliers = numOutliers;
        this.creationTimeMs = creationTimeMs;
    }

    /**
     * Removes redundant explanations
     * @return New explanation with redundant itemsets removed.
     */
    public FPGExplanation prune() {
        itemsets.sort((FPGAttributeSet a, FPGAttributeSet b) -> -a.compareTo(b));
        List<FPGAttributeSet> newItemsets = new ArrayList<>();
        int n = itemsets.size();
        for (int i = 0; i < n; i++) {
            FPGAttributeSet aSet = itemsets.get(i);
            boolean redundant = false;
            // an explanation is redundant if it has lower risk ratio (occurs after since sorted)
            // than an explanation that involves a subset of the same attributes
            for (int j = 0; j < i; j++) {
                FPGAttributeSet comparisonSet = itemsets.get(j);
                if (aSet.contains(comparisonSet)) {
                    redundant = true;
                    break;
                }
            }
            if (!redundant) {
                newItemsets.add(aSet);
            }
        }

        FPGExplanation newExplanation = new FPGExplanation(
                newItemsets,
                numInliers,
                numOutliers,
                creationTimeMs
        );
        return newExplanation;
    }

    public void sortByRiskRatio() {
        itemsets.sort((FPGAttributeSet a, FPGAttributeSet b) -> -a.compareTo(b));
    }

    public void sortBySupport() {
        itemsets.sort((FPGAttributeSet a, FPGAttributeSet b) -> -Double.compare(a.getSupport(),b.getSupport()));
    }

    public List<FPGAttributeSet> getItemsets() {
        return itemsets;
    }

    public double numOutliers() { return (double)numOutliers;}
    public double numTotal() {return (double)numOutliers + numInliers;}
    public long getNumOutliers() {
        return numOutliers;
    }
    public long getNumInliers() {
        return numInliers;
    }
    public long getCreationTimeMs() {
        return creationTimeMs;
    }

    @Override
    public String prettyPrint() {
        StringBuilder header = new StringBuilder(String.format(
                "Outlier Explanation:\n"
                + "numOutliers: %d\n"
                + "numInliers: %d\n"
                + "Itemsets: \n"
                + "--------\n",
                numOutliers,
                numInliers));
        for (FPGAttributeSet is : itemsets) {
            header.append(is.prettyPrint());
        }
        return header.toString();
    }

    @Override
    public String toString() {
        return "Explanation{" +
                "numOutliers=" + numOutliers +
                ", numInliers=" + numInliers +
                ", itemsets=" + itemsets +
                '}';
    }
}
