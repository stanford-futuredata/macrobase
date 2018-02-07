package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGAttributeSet;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGItemsetResult;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Given a batch of rows with an outlier class column, explain the outliers using
 * string attribute columns. Each batch is considered as an independent unit.
 */
public class FPGrowthSummarizer extends BatchSummarizer {
    protected double minRiskRatio = 3;
    // Encoder
    protected AttributeEncoder encoder = new AttributeEncoder();
    private boolean useAttributeCombinations = true;

    // Output
    private FPGExplanation explanation = null;
    private List<Set<Integer>> inlierItemsets, outlierItemsets;
    private FPGrowthEmerging fpg = new FPGrowthEmerging();

    public FPGrowthSummarizer() { }

    /**
     * Whether or not to use combinations of attributes in explanation, or only
     * use simple single attribute explanations
     * @param useAttributeCombinations flag
     * @return this
     */
    public FPGrowthSummarizer setUseAttributeCombinations(boolean useAttributeCombinations) {
        this.useAttributeCombinations = useAttributeCombinations;
        fpg.setCombinationsEnabled(useAttributeCombinations);
        return this;
    }

    @Override
    public void process(DataFrame df) {
        // Filter inliers and outliers
        DataFrame outlierDF = df.filter(outlierColumn, (double d) -> d > 0.0);
        DataFrame inlierDF = df.filter(outlierColumn, (double d) -> d == 0.0);

        // Encode inlier and outlier attribute columns
        if (attributes.isEmpty()) {
            encoder.setColumnNames(df.getSchema().getColumnNamesByType(Schema.ColType.STRING));
            inlierItemsets = encoder.encodeAttributesAsSets(inlierDF.getStringCols());
            outlierItemsets = encoder.encodeAttributesAsSets(outlierDF.getStringCols());
        } else {
            encoder.setColumnNames(attributes);
            inlierItemsets = encoder.encodeAttributesAsSets(inlierDF.getStringColsByName(attributes));
            outlierItemsets = encoder.encodeAttributesAsSets(outlierDF.getStringColsByName(attributes));
        }

        long startTime = System.currentTimeMillis();
        List<FPGItemsetResult> itemsetResults = fpg.getEmergingItemsetsWithMinSupport(
                inlierItemsets,
                outlierItemsets,
                minOutlierSupport,
                minRiskRatio);
        // Decode results
        List<FPGAttributeSet> attributeSets = new ArrayList<>();
        itemsetResults.forEach(i -> attributeSets.add(new FPGAttributeSet(i, encoder)));
        long elapsed = System.currentTimeMillis() - startTime;

        explanation = new FPGExplanation(attributeSets,
                inlierItemsets.size(),
                outlierItemsets.size(),
                elapsed);
    }

    @Override
    public FPGExplanation getResults() {
        return explanation;
    }

    /**
     * Adjust this to tune the severity (e.g. strength of correlation) of the results returned.
     * @param minRiskRatio lowest risk ratio to consider for meaningful explanations.
     * @return this
     */
    public BatchSummarizer setMinRiskRatio(double minRiskRatio) {
        this.minRiskRatio = minRiskRatio;
        return this;
    }
}
