package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a summarization result, which contains a list of attribute values
 * and other statistics about the underlying process, e.g. num of tuples observed
 * so far.
 */
public class Explanation {
    private final long numOutliers;
    private final long numInliers;
    private List<AttributeSet> itemsets;
    private final long creationTimeMs;

    public Explanation(List<AttributeSet> resultList,
                       long numInliers,
                       long numOutliers,
                       long creationTimeMs) {
        itemsets = new ArrayList<>(resultList);
        itemsets.sort((AttributeSet a, AttributeSet b) -> -a.compareTo(b));
        this.numInliers = numInliers;
        this.numOutliers = numOutliers;
        this.creationTimeMs = creationTimeMs;
    }

    /**
     * Removes redundant explanations
     * @return New explanation with redundant itemsets removed.
     */
    public Explanation prune() {
        List<AttributeSet> newItemsets = new ArrayList<>();
        int n = itemsets.size();
        for (int i = 0; i < n; i++) {
            AttributeSet aSet = itemsets.get(i);
            boolean redundant = false;
            // an explanation is redundant if it has lower risk ratio (occurs after since sorted)
            // than an explanation that involves a subset of the same attributes
            for (int j = 0; j < i; j++) {
                AttributeSet comparisonSet = itemsets.get(j);
                if (aSet.contains(comparisonSet)) {
                    redundant = true;
                    break;
                }
            }
            if (!redundant) {
                newItemsets.add(aSet);
            }
        }

        Explanation newExplanation = new Explanation(
                newItemsets,
                numInliers,
                numOutliers,
                creationTimeMs
        );
        return newExplanation;
    }

    public List<AttributeSet> getItemsets() {
        return itemsets;
    }

    public long getNumOutliers() {
        return numOutliers;
    }

    public long getNumInliers() {
        return numInliers;
    }

    public long getCreationTimeMs() {
        return creationTimeMs;
    }

    public String prettyPrint() {
        StringBuilder header = new StringBuilder(String.format(
                "Outlier Explanation:\n"
                + "numOutliers: %d\n"
                + "numInliners: %d\n"
                + "Itemsets: \n"
                + "--------\n",
                numOutliers,
                numInliers,
                itemsets));
        for (AttributeSet is : itemsets) {
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
