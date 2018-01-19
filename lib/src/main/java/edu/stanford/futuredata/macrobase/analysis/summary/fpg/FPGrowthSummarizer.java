package edu.stanford.futuredata.macrobase.analysis.summary.fpg;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGAttributeSet;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGItemsetResult;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.GlobalRatioMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.SupportMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Given a batch of rows with an outlier class column, explain the outliers using
 * string attribute columns. Each batch is considered as an independent unit.
 */
public class FPGrowthSummarizer extends BatchSummarizer {
    private double minRiskRatio = 3;
    // Encoder
    private AttributeEncoder encoder = new AttributeEncoder();

    // Output
    private FPGExplanation explanation = null;
    private FPGrowthEmerging fpg = new FPGrowthEmerging();

    public FPGrowthSummarizer() { }

    /**
     * Whether or not to use combinations of attributes in explanation, or only
     * use simple single attribute explanations
     * @param useAttributeCombinations flag
     */
    public void setUseAttributeCombinations(boolean useAttributeCombinations) {
        fpg.setCombinationsEnabled(useAttributeCombinations);
    }

    /**
     * Adjust this to tune the severity (e.g. strength of correlation) of the results returned.
     * @param minRiskRatio lowest risk ratio to consider for meaningful explanations.
     */
    public void setMinRiskRatio(double minRiskRatio) {
        this.minRiskRatio = minRiskRatio;
    }

    private List<QualityMetric> getQualityMetricList() {
        List<QualityMetric> qualityMetricList = new ArrayList<>();
        qualityMetricList.add(
                new GlobalRatioMetric(0, 1)
        );
        return qualityMetricList;
    }

    private List<Double> getThresholds() {
        return Arrays.asList(minRiskRatio);
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
            getQualityMetricList(),
            getThresholds(),
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
