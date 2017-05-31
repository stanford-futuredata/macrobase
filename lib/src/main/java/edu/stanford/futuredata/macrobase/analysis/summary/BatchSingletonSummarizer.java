package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.summary.itemset.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.FPGrowthEmerging;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.ItemsetResult;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.operator.Operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.DoublePredicate;

/**
 * Given a batch of rows with an outlier class column, explain the outliers using
 * string attribute columns. Each batch is considered as an independent unit.
 */
public class BatchSingletonSummarizer implements Operator<DataFrame, Explanation> {
    // Parameters
    private String outlierColumn = "_OUTLIER";
    private double minOutlierSupport = 0.1;
    private double minRiskRatio = 3;
    private boolean useAttributeCombinations = true;
    private List<String> attributes = new ArrayList<>();
    private DoublePredicate predicate = d -> d != 0.0;

    // Output
    private Explanation explanation = null;
    // Encoder
    private AttributeEncoder encoder = new AttributeEncoder();
    private List<Set<Integer>> inlierItemsets, outlierItemsets;
    private FPGrowthEmerging fpg = new FPGrowthEmerging();

    public BatchSingletonSummarizer() { }

    /**
     * Adjust this to tune the significance (e.g. number of rows affected) of the results returned.
     * @param minSupport lowest outlier support of the results returned.
     * @return this
     */
    public BatchSingletonSummarizer setMinSupport(double minSupport) {
        this.minOutlierSupport = minSupport;
        return this;
    }

    /**
     * Adjust this to tune the severity (e.g. strength of correlation) of the results returned.
     * @param minRiskRatio lowest risk ratio to consider for meaningful explanations.
     * @return this
     */
    public BatchSingletonSummarizer setMinRiskRatio(double minRiskRatio) {
        this.minRiskRatio = minRiskRatio;
        return this;
    }

    /**
     * By default, will check for nonzero entries in a column of doubles.
     * @param predicate function to signify whether row should be treated as outlier.
     * @return this
     */
    public BatchSingletonSummarizer setOutlierPredicate(DoublePredicate predicate) {
        this.predicate = predicate;
        return this;
    }
    public BatchSingletonSummarizer setAttributes(List<String> attributes) {
        this.attributes = attributes;
        this.encoder.setColumnNames(attributes);
        return this;
    }

    /**
     * Set the column which indicates outlier status. "_OUTLIER" by default.
     * @param outlierColumn new outlier indicator column.
     * @return this
     */
    public BatchSingletonSummarizer setOutlierColumn(String outlierColumn) {
        this.outlierColumn = outlierColumn;
        return this;
    }

    /**
     * Whether or not to use combinations of attributes in explanation, or only
     * use simple single attribute explanations
     * @param useAttributeCombinations flag
     * @return this
     */
    public BatchSingletonSummarizer setUseAttributeCombinations(boolean useAttributeCombinations) {
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
