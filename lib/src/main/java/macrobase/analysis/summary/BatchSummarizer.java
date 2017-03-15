package macrobase.analysis.summary;

import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.AttributeEncoder;
import macrobase.analysis.summary.itemset.result.AttributeSet;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.conf.MacrobaseException;
import macrobase.datamodel.DataFrame;
import macrobase.datamodel.Schema;
import macrobase.operator.Operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.DoublePredicate;

public class BatchSummarizer implements Operator<DataFrame, Explanation> {
    // Parameters
    public String outlierColumn = "_OUTLIER";
    public double minOutlierSupport = 0.1;
    public double minRiskRatio = 3;
    public boolean useAttributeCombinations = false;
    public List<String> attributes = new ArrayList<>();
    public DoublePredicate predicate = d -> d != 0.0;

    // Output
    private Explanation explanation = null;
    // Encoder
    private AttributeEncoder encoder = new AttributeEncoder();
    private List<Set<Integer>> inlierItemsets, outlierItemsets;
    private FPGrowthEmerging fpg = new FPGrowthEmerging();

    // Setter and constructor
    public BatchSummarizer() { }
    public BatchSummarizer enableAttributeCombinations() {
        this.useAttributeCombinations = true;
        fpg.enableCombination();
        return this;
    }
    public BatchSummarizer setMinSupport(double minSupport) {
        this.minOutlierSupport = minSupport;
        return this;
    }
    public BatchSummarizer setMinRiskRatio(double minRiskRatio) {
        this.minRiskRatio = minRiskRatio;
        return this;
    }
    public BatchSummarizer setOutlierPredicate(DoublePredicate predicate) {
        this.predicate = predicate;
        return this;
    }
    public BatchSummarizer setAttributes(List<String> attributes) {
        this.attributes = attributes;
        this.encoder.setColumnNames(attributes);
        return this;
    }
    public BatchSummarizer setOutlierColumn(String outlierColumn) {
        this.outlierColumn = outlierColumn;
        return this;
    }

    @Override
    public void process(DataFrame df) throws MacrobaseException {
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
