package macrobase.analysis.summary;

import macrobase.analysis.summary.itemset.result.ItemsetResult;

import java.util.List;

/**
 * Represents a summarization result, which contains a List of ItemsetResults
 * and other statistics about the underlying process, e.g. num of tuples observed
 * so far.
 */
public class Summary {
    private final long numOutliers;
    private final long numInliers;
    private List<ItemsetResult> itemsets;
    private final long creationTimeMs;

    public Summary(List<ItemsetResult> resultList,
                   long numInliers,
                   long numOutliers,
                   long creationTimeMs) {
        itemsets = resultList;
        this.numInliers = numInliers;
        this.numOutliers = numOutliers;
        this.creationTimeMs = creationTimeMs;
    }

    public List<ItemsetResult> getItemsets() {
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
                "Outlier Summary:\n"
                + "numOutliers: %d\n"
                + "numInliners: %d\n"
                + "Itemsets: \n"
                + "--------\n",
                numOutliers,
                numInliers,
                itemsets));
        for (ItemsetResult is : itemsets) {
            header.append(is.prettyPrint());
        }
        return header.toString();
    }

    @Override
    public String toString() {
        return "Summary{" +
                "numOutliers=" + numOutliers +
                ", numInliers=" + numInliers +
                ", itemsets=" + itemsets +
                '}';
    }
}
