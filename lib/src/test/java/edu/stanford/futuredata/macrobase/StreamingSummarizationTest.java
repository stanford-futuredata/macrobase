package edu.stanford.futuredata.macrobase;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.IncrementalSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Test;

import java.util.*;

public class StreamingSummarizationTest {
    /**
     * Generates a simulated labeled dataset with both a random noise floor of outliers
     * and a "bug" which shows up partway through and affects a specific combination of
     * attribute values. Useful for testing streaming summarization operators.
     *
     * @param n number of rows
     * @param k k-way simultaneous bad attributes required for bug
     * @param C cardinality of each attribute column
     * @param d dimension of number of attribute columns
     * @param p probability of a random row being an outlier
     * @param changeStartIdx index at which the "bug" starts showing up
     * @return Complete dataset
     */
    public static DataFrame generateAnomalyDataset(
            int n, int k, int C, int d, double p,
            int changeStartIdx
    ) {
        Random r = new Random(0);

        double[] time = new double[n];
        String[][] attrs = new String[d][n];
        double[] isOutlier = new double[n];
        for (int i = 0; i < n; i++) {
            double curTime = i;
            time[i] = curTime;

            int[] attrValues = new int[d];
            for (int j = 0; j < d; j++) {
                attrValues[j] = r.nextInt(C);
                attrs[j][i] = String.format("a%d:%d", j, attrValues[j]);
            }

            // Outliers arise from random noies
            // and also from matching key combination after the "event" occurs
            boolean curOutlier = r.nextFloat() < p;
            if (i >= changeStartIdx) {
                boolean match = true;
                for (int j = 0; j < k; j++) {
                    if (attrValues[j] != 1) {
                        match = false;
                    }
                }
                if (match) {
                    curOutlier = true;
                }
            }
            isOutlier[i] = curOutlier ? 1.0 : 0.0;
        }

        DataFrame df = new DataFrame();
        df.addDoubleColumn("time", time);
        for (int j = 0; j < d; j++) {
            df.addStringColumn("a"+j, attrs[j]);
        }
        df.addDoubleColumn("outlier", isOutlier);
        return df;
    }

    public static Set<String> generateTrueAnomalousAttribute(int k) {
        Set<String> values = new HashSet<>(k);
        for (int i = 0; i < k; i ++) {
            values.add("a" + i + ":1");
        }
        return values;
    }

    @Test
    public void testDetectSingleChange() throws Exception {
        int n = 10000;
        int k = 2;
        int C = 4;
        int d = 5;
        double p = 0.01;
        int eventIdx = 2000;
        List<String> attributes = new ArrayList<>();
        for (int i = 0; i < d; i++) {
            attributes.add("a"+i);
        }
        DataFrame df = generateAnomalyDataset(n, k, C, d, p, eventIdx);
        Set<String> grouthTruthAttributes = generateTrueAnomalousAttribute(k);

        int slideSize = 500;
        // Test singleton summarizer
        IncrementalSummarizer singletonSummarizer = new IncrementalSummarizer(n / slideSize);
        singletonSummarizer.setOutlierColumn("outlier").setAttributes(attributes).setUseAttributeCombinations(false);
        // Test summarizer with combinations
        IncrementalSummarizer summarizer = new IncrementalSummarizer(n / slideSize);
        summarizer.setOutlierColumn("outlier").setAttributes(attributes);

        double startTime = 0.0;

        while (startTime < n) {
            double endTime = startTime + slideSize;
            double ls = startTime;
            DataFrame curBatch = df.filter("time", (double t) -> t >= ls && t < endTime);
            summarizer.process(curBatch);
            singletonSummarizer.process(curBatch);
            Explanation singleExplanation = singletonSummarizer.getResults();
            Explanation explanation = summarizer.getResults();

            assert (explanation.getNumInliers() + explanation.getNumOutliers() == endTime);
            assert (singleExplanation.getNumInliers() + singleExplanation.getNumOutliers() == endTime);

            if (startTime < eventIdx) { // No results before event time
                assert (explanation.getItemsets().size() == 0);
                assert (singleExplanation.getItemsets().size() == 0);
            } else {
                // Singleton:
                //     make sure that each anomalous attribute is picked out by the summarizer
                assert (singleExplanation.getItemsets().size() == k);
                Set<String> values = new HashSet<>();
                for (int i = 0; i < k; i ++) {
                    assert (explanation.getItemsets().get(i).getItems().size() == 1);
                    values.addAll(explanation.getItemsets().get(i).getItems().values());
                }
                assert (values.containsAll(grouthTruthAttributes));

                // Combination:
                //    make sure that the known anomalous attribute combination has the highest risk ratio
                List<AttributeSet> comboAttributes = explanation.getItemsets();
                Collections.sort(comboAttributes,
                        (a, b)->(new Double(b.getRatioToInliers()).compareTo(new Double(a.getRatioToInliers()))));
                assert(comboAttributes.get(0).getItems().values().containsAll(grouthTruthAttributes));

                // Check whether the final result from the incremental summarizer agrees with the batch summarizer
                if (endTime == n) {
                    BatchSummarizer batch = new BatchSummarizer().setAttributes(attributes).setOutlierColumn("outlier");
                    batch.process(df.filter("time", (double t) -> t >= eventIdx));
                    Explanation batchResult = batch.getResults();
                    for (AttributeSet expected : batchResult.getItemsets()) {
                        if (expected.getItems().values().containsAll(grouthTruthAttributes)) { // Check match for combination
                            assert(expected.getNumRecords() == comboAttributes.get(0).getNumRecords());
                        } else if (expected.getItems().keySet().size() == 1) { // Check match for singletons
                            for (int i = 0; i < k; i ++) {
                                AttributeSet singleAttribute = singleExplanation.getItemsets().get(i);
                                if (singleAttribute.getItems().equals(expected.getItems())) {
                                    assert(singleAttribute.getNumRecords() == expected.getNumRecords());
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            startTime = endTime;
        }
    }
}
