package edu.stanford.futuredata.macrobase;

import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.ItemsetBatchSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test looks at data with 1000 inliers and 20 outliers.
 * The outliers have lower usage and all have
 * location=CAN, version=v3
 */
public class UnsupervisedCSVTest {
    private DataFrame df;

    @Before
    public void setUp() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        schema.put("usage", Schema.ColType.DOUBLE);
        schema.put("latency", Schema.ColType.DOUBLE);
        schema.put("location", Schema.ColType.STRING);
        schema.put("version", Schema.ColType.STRING);
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/sample.csv"
        ).setColumnTypes(schema);
        df = loader.load();
    }

    @Test
    public void testGetSummaries() throws Exception {
        PercentileClassifier pc = new PercentileClassifier("usage")
                .setPercentile(1.0);
        pc.process(df);
        DataFrame df_classified = pc.getResults();

        List<String> explanationAttributes = Arrays.asList(
                "location",
                "version"
        );
        ItemsetBatchSummarizer summ = new ItemsetBatchSummarizer()
                .setAttributes(explanationAttributes);
        summ.process(df_classified);
        Explanation results = summ.getResults();
        assertEquals(3, results.getItemsets().size());
    }

    @Test
    public void testCustomizedSummaries() throws Exception {
        PercentileClassifier pc = new PercentileClassifier("usage")
                .setPercentile(1.0);
        pc.process(df);
        DataFrame df_classified = pc.getResults();

        List<String> explanationAttributes = Arrays.asList(
                "location",
                "version"
        );
        // Increase risk ratio
        ItemsetBatchSummarizer summ = new ItemsetBatchSummarizer()
                .setAttributes(explanationAttributes)
                .setMinRiskRatio(5.0);
        summ.process(df_classified);
        Explanation results = summ.getResults();
        assertEquals(1, results.getItemsets().size());

        // Increase support requirement
        summ = new ItemsetBatchSummarizer()
                .setAttributes(explanationAttributes)
                .setMinSupport(0.55);
        summ.process(df_classified);
        results = summ.getResults();
        assertEquals(2, results.getItemsets().size());

        // Restrict to only simple explanations
        summ = new ItemsetBatchSummarizer()
                .setAttributes(explanationAttributes)
                .setUseAttributeCombinations(false);
        summ.process(df_classified);
        results = summ.getResults();
        assertEquals(2, results.getItemsets().size());

        // Invert outlier classes
        summ = new ItemsetBatchSummarizer()
                .setAttributes(explanationAttributes)
                .setOutlierPredicate(d -> d == 0.0);
        summ.process(df_classified);
        results = summ.getResults();
        assertEquals(1000, results.getNumOutliers());
        assertEquals(0, results.getItemsets().size());
    }

    @Test
    public void testCustomizedClassifier() throws Exception {
        PercentileClassifier pc = new PercentileClassifier("usage")
                .setPercentile(2.0)
                .setIncludeHigh(false)
                .setIncludeLow(true);
        pc.process(df);
        DataFrame df_classified = pc.getResults();

        List<String> explanationAttributes = Arrays.asList(
                "location",
                "version"
        );
        ItemsetBatchSummarizer summ = new ItemsetBatchSummarizer()
                .setAttributes(explanationAttributes)
                .setUseAttributeCombinations(false);
        summ.process(df_classified);
        Explanation results = summ.getResults();
        assertEquals(2, results.getItemsets().size());
        assertTrue(results.getItemsets().get(0).getSupport() > .9);
    }
}
