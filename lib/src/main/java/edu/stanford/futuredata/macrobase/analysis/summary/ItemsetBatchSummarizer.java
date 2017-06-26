package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.summary.itemset.FPGrowthEmerging;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.ItemsetResult;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Given a batch of rows with an outlier class column, explain the outliers using
 * string attribute columns. Each batch is considered as an independent unit.
 */
public class ItemsetBatchSummarizer extends BatchSummarizer {
    private boolean useAttributeCombinations = true;

    // Output
    private Explanation explanation = null;
    private List<Set<Integer>> inlierItemsets, outlierItemsets;
    private FPGrowthEmerging fpg = new FPGrowthEmerging();

    public ItemsetBatchSummarizer() { }

    /**
     * Whether or not to use combinations of attributes in explanation, or only
     * use simple single attribute explanations
     * @param useAttributeCombinations flag
     * @return this
     */
    public ItemsetBatchSummarizer setUseAttributeCombinations(boolean useAttributeCombinations) {
        this.useAttributeCombinations = useAttributeCombinations;
        fpg.setCombinationsEnabled(useAttributeCombinations);
        return this;
    }

    @Override
    public void process(DataFrame df) {
        // Filter inliers and outliers
        DataFrame outlierDF = df.filter(outlierColumn, predicate);
        DataFrame inlierDF = df.filter(outlierColumn, predicate.negate());

        // Encode inlier and outlier attribute columns
        if (attributes.isEmpty()) {
            encoder.setColumnNames(df.getSchema().getColumnNamesByType(Schema.ColType.STRING));
            inlierItemsets = encoder.encodeAttributes(inlierDF.getStringCols());
            outlierItemsets = encoder.encodeAttributes(outlierDF.getStringCols());
        } else {
            encoder.setColumnNames(attributes);
            inlierItemsets = encoder.encodeAttributes(inlierDF.getStringColsByName(attributes));
            outlierItemsets = encoder.encodeAttributes(outlierDF.getStringColsByName(attributes));
        }

        long startTime = System.currentTimeMillis();
        List<ItemsetResult> itemsetResults = fpg.getEmergingItemsetsWithMinSupport(
            inlierItemsets,
            outlierItemsets,
            minOutlierSupport,
            minRiskRatio);
        // Decode results
        List<AttributeSet> attributeSets = new ArrayList<>();
        itemsetResults.forEach(i -> attributeSets.add(new AttributeSet(i, encoder)));
        long elapsed = System.currentTimeMillis() - startTime;

        explanation = new Explanation(attributeSets,
                inlierItemsets.size(),
                outlierItemsets.size(),
                elapsed);
    }

    @Override
    public Explanation getResults() {
        return explanation;
    }
}
