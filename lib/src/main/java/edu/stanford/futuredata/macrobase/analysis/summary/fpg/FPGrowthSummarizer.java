package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGAttributeSet;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGItemsetResult;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.RiskRatioQualityMetric;
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
    // Encoder
    private AttributeEncoder encoder = new AttributeEncoder();

    // Output
    private FPGExplanation explanation = null;
    private FPGrowthEmerging fpg = new FPGrowthEmerging();
    /**
     * A qualityMetric for an attribute is a function which takes in the number of outliers
     * and the total number of examples with that attribute and returns a value.  A
     * qualityMetric is passed if that value is greater than a corresponding threshold.
     */
    private List<QualityMetric> qualityMetricList = new ArrayList<>();
    /**
     * Thresholds corresponding to the qualityMetrics.
     */
    private List<Double> thresholds = new ArrayList<>();

    /**
     * Default to a RiskRatioQualityMetric with a minRiskRatio of 3.
     */
    public FPGrowthSummarizer() {
        qualityMetricList.add(
                new RiskRatioQualityMetric(0, 1)
        );
        thresholds.add(3.0);
    }

    /**
     * Whether or not to use combinations of attributes in explanation, or only
     * use simple single attribute explanations
     * @param useAttributeCombinations flag
     */
    public void setUseAttributeCombinations(boolean useAttributeCombinations) {
        fpg.setCombinationsEnabled(useAttributeCombinations);
    }

    /**
     * Set the qualityMetrics to use and their corresponding thresholds.
     * @param newQualityMetrics The new qualityMetrics.
     * @param newThresholds The new thresholds.
     */
    public void setQualityMetrics(List<QualityMetric> newQualityMetrics,
                                   List<Double> newThresholds) {
        this.qualityMetricList = newQualityMetrics;
        this.thresholds = newThresholds;
    }

    @Override
    public void process(DataFrame df) {
        // Filter inliers and outliers
        DataFrame outlierDF = df.filter(outlierColumn, (double d) -> d > 0.0);
        DataFrame inlierDF = df.filter(outlierColumn, (double d) -> d == 0.0);
        List<Set<Integer>> inlierItemsets, outlierItemsets;

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
                qualityMetricList,
                thresholds,
            minOutlierSupport);
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
}
