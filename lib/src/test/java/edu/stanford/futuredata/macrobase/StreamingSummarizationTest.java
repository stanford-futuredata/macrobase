package edu.stanford.futuredata.macrobase;

import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.IncrementalSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGAttributeSet;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.WindowedOperator;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamingSummarizationTest {
    /**
     * Generates a simulated labeled dataset with both a random noise floor of outliers
     * and a "bug" which shows up partway through and affects a specific combination of
     * attribute values. Useful for testing streaming summarization operators.
     *
     * @param n              number of rows
     * @param k              k-way simultaneous bad attributes required for bug
     * @param C              cardinality of each attribute column
     * @param d              dimension of number of attribute columns
     * @param p              probability of a random row being an outlier
     * @param changeStartIdx index at which the "bug" starts showing up
     * @param changeEndIdx   index at which the "bug" ends showing up
     * @return Complete dataset
     */
    public static DataFrame generateAnomalyDataset(
            int n, int k, int C, int d, double p,
            int changeStartIdx, int changeEndIdx) {
        Random r = new Random(0);

        double[] time = new double[n];
        String[][] attrs = new String[d][n];
        double[] isOutlier = new double[n];

        String[][] attrPrimitiveValues = new String[d][C];
        for (int i = 0; i < C; i++) {
            for (int j = 0; j < d; j++) {
                attrPrimitiveValues[j][i] = String.format("a%d:%d", j, i);
            }
        }

        for (int i = 0; i < n; i++) {
            double curTime = i;
            time[i] = curTime;

            int[] attrValues = new int[d];
            for (int j = 0; j < d; j++) {
                attrValues[j] = r.nextInt(C);
                attrs[j][i] = attrPrimitiveValues[j][attrValues[j]];
            }

            // Outliers arise from random noies
            // and also from matching key combination after the "event" occurs
            boolean curOutlier = r.nextFloat() < p;
            if (i >= changeStartIdx && i < changeEndIdx) {
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
        df.addColumn("time", time);
        for (int j = 0; j < d; j++) {
            df.addColumn("a" + j, attrs[j]);
        }
        df.addColumn("outlier", isOutlier);
        return df;
    }

    public static List<String> getAttributes(int d, boolean getValue) {
        List<String> attributes = new ArrayList<>(d);
        for (int i = 0; i < d; i++) {
            if (getValue) {
                attributes.add("a" + i + ":1");
            } else {
                attributes.add("a" + i);
            }
        }
        return attributes;
    }

    @Test
    public void testDetectSingleChange() throws Exception {
        // Prepare data set
        int n = 7000;
        int k = 2;
        int C = 5;
        int d = 5;
        double p = 0.01;
        int eventIdx = 2000;
        int eventEndIdx = 4000;
        int windowSize = 2000;
        int slideSize = 1000;

        List<String> attributes = getAttributes(d, false);
        List<String> buggyAttributeValues = getAttributes(k, true);
        DataFrame df = generateAnomalyDataset(n, k, C, d, p, eventIdx, eventEndIdx);

        IncrementalSummarizer outlierSummarizer = new IncrementalSummarizer();
        outlierSummarizer.setAttributes(attributes);
        outlierSummarizer.setOutlierColumn("outlier");
        outlierSummarizer.setMinSupport(.5);
        WindowedOperator<FPGExplanation> windowedSummarizer = new WindowedOperator<>(outlierSummarizer);
        windowedSummarizer.setWindowLength(windowSize);
        windowedSummarizer.setTimeColumn("time");
        windowedSummarizer.setSlideLength(slideSize);
        windowedSummarizer.initialize();

        double miniBatchSize = slideSize;
        double startTime = 0.0;
        while (startTime < n) {
            double endTime = startTime + miniBatchSize;
            double ls = startTime;
            DataFrame curBatch = df.filter("time", (double t) -> t >= ls && t < endTime);

            /* Code to process windowed summarizer on a minibatch */
            windowedSummarizer.process(curBatch);
            FPGExplanation explanation = windowedSummarizer.getResults().prune();
            /* End */

            if (endTime > windowSize) {
                assertEquals(windowSize, explanation.getNumInliers() + explanation.getNumOutliers());
            }

            if (windowedSummarizer.getMaxWindowTime() > eventIdx
                    && windowedSummarizer.getMaxWindowTime() - windowSize < eventEndIdx) {
                //  make sure that the known anomalous attribute combination has the highest risk ratio
                FPGAttributeSet topRankedExplanation = explanation.getItemsets().get(0);
                assertTrue(topRankedExplanation.getItems().values().containsAll(buggyAttributeValues));
            } else {
                // Otherwise make sure that the noisy explanations are all low-cardinality
                if (explanation.getItemsets().size() > 0) {
                    FPGAttributeSet topRankedExplanation = explanation.getItemsets().get(0);
                    assertTrue(
                            topRankedExplanation.getNumRecords() < 20
                    );
                }
            }
            startTime = endTime;

        }
    }
}
