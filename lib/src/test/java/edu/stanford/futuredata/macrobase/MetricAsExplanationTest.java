package edu.stanford.futuredata.macrobase;

import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGrowthSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGAttributeSet;
import edu.stanford.futuredata.macrobase.analysis.transform.MetricBucketTransformer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetricAsExplanationTest {
    /**
     * @param n Number of rows
     * @param d Number of metric columns
     * @param k Number of correlated columns
     * @param corr Degree of correlation
     * @return DataFrame with correlated columns
     */
    public static DataFrame generateCorrelatedMetricDataset(
            int n, int d, int k, double corr
    ) {
        Random r = new Random(0);
        DataFrame df = new DataFrame();

        double[] baseMetric = new double[n];
        for (int i = 0; i < n; i++) {
            baseMetric[i] = r.nextGaussian();
        }

        for (int j = 0; j < d; j++) {
            double[] metric = new double[n];
            for (int i = 0; i < n; i++) {
                if (j < k) {
                    metric[i] = corr*baseMetric[i] + (1-corr)*r.nextGaussian();
                } else {
                    metric[i] = r.nextGaussian();
                }
            }
            df.addColumn("m"+j,metric);
        }
        return df;
    }

    @Test
    public void testFindMetricExplanation() throws Exception {
        int d = 6;
        DataFrame df = generateCorrelatedMetricDataset(10000, d, 3, .95);

        PercentileClassifier c = new PercentileClassifier("m0");
        c.setPercentile(2.0);
        c.process(df);
        DataFrame cdf = c.getResults();

        List<String> metricExplanationColumns = new ArrayList<>();
        for (int i = 1; i < d; i++) {
            metricExplanationColumns.add("m"+i);
        }
        MetricBucketTransformer t = new MetricBucketTransformer(metricExplanationColumns);
        double[] boundaries = {5.0, 95.0};
        t.setBoundaryPercentiles(boundaries);
        t.process(cdf);
        // tcdf now contains new columns with metrics transformed into bucketed attributes
        DataFrame tcdf = t.getResults();

        // Need to retrieve the new names of the transformed columns
        List<String> bucketExplanationColumns = t.getTransformedColumnNames();

        FPGrowthSummarizer bs = new FPGrowthSummarizer();
        bs.setOutlierColumn(c.getOutputColumnName());
        bs.setUseAttributeCombinations(true);
        bs.setAttributes(bucketExplanationColumns);
        bs.setMinRiskRatio(2.0);
        bs.setMinSupport(.3);
        bs.process(tcdf);
        FPGExplanation e = bs.getResults();

        List<FPGAttributeSet> results = e.getItemsets();
        // Top result should involve both explanatory metrics
        assertTrue(results.get(0).getItems().keySet().containsAll(Arrays.asList("m1_a", "m2_a")));
        assertTrue(results.size() >= 4);
        results.get(0).getItems().keySet().contains("m1_a");
    }
}
