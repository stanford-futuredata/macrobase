package edu.stanford.futuredata.macrobase;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.IncrementalSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.WindowedOperator;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertTrue;

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
     * @param changeEndIdx index at which the "bug" ends showing up
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
                attrPrimitiveValues[j][i] = String.format("a%d:%d",j,i);
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
        df.addDoubleColumn("time", time);
        for (int j = 0; j < d; j++) {
            df.addStringColumn("a"+j, attrs[j]);
        }
        df.addDoubleColumn("outlier", isOutlier);
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

    /**
     * Test the baseline performance of a windowed, streaming, summarizer operator
     */
    @Test
    public void testStreamingWindowed() throws Exception {
        // Increase these numbers for more rigorous, slower performance testing
        int n = 6000;
        int k = 2;
        int C = 4;
        int d = 5;
        double p = 0.01;
        int eventIdx = 2000;
        int eventEndIdx = 4000;
        int windowSize = 2000;
        int slideSize = 300;

        DataFrame df = generateAnomalyDataset(n, k, C, d, p, eventIdx, eventEndIdx);
        List<String> attributes = getAttributes(d, false);

        IncrementalSummarizer outlierSummarizer = new IncrementalSummarizer();
        outlierSummarizer.setAttributes(attributes);
        outlierSummarizer.setOutlierColumn("outlier");
        outlierSummarizer.setMinSupport(.3);
        WindowedOperator<Explanation> windowedSummarizer = new WindowedOperator<>(outlierSummarizer);
        windowedSummarizer.setWindowLength(windowSize);
        windowedSummarizer.setTimeColumn("time");
        windowedSummarizer.setSlideLength(slideSize);
        windowedSummarizer.initialize();

        BatchSummarizer bsumm = new BatchSummarizer();
        bsumm.setAttributes(attributes);
        bsumm.setOutlierColumn("outlier");
        bsumm.setMinSupport(.3);

        int miniBatchSize = slideSize;
        double totalStreamingTime = 0.0;
        double totalBatchTime = 0.0;

        double startTime = 0.0;
        while (startTime < n) {
            double endTime = startTime + miniBatchSize;
            double ls = startTime;
            DataFrame curBatch = df.filter(
                    "time",
                    (double t) -> t >= ls && t < endTime
            );
            long timerStart = System.currentTimeMillis();
            windowedSummarizer.process(curBatch);
            Explanation curExplanation = windowedSummarizer.getResults();
            long timerElapsed = System.currentTimeMillis() - timerStart;
            totalStreamingTime += timerElapsed;

            DataFrame curWindow = df.filter(
                    "time",
                    (double t) -> t >= (endTime - windowSize) && t < endTime
            );
            timerStart = System.currentTimeMillis();
            bsumm.process(curWindow);
            Explanation batchExplanation = bsumm.getResults();
            timerElapsed = System.currentTimeMillis() - timerStart;
            totalBatchTime += timerElapsed;

            startTime = endTime;
        }

        assertTrue(totalStreamingTime < totalBatchTime);
    }

    @Test
    public void testDetectSingleChange() throws Exception {
        // Prepare data set
        int n = 10000;
        int k = 2;
        int C = 4;
        int d = 5;
        double p = 0.01;
        int eventIdx = 2000;
        int eventEndIdx = 8000;
        int windowSize = 6000;
        int slideSize = 500;

        List<String> attributes = getAttributes(d, false);
        List<String> buggyAttributeValues = getAttributes(k, true);
        DataFrame df = generateAnomalyDataset(n, k, C, d, p, eventIdx, eventEndIdx);
        BatchSummarizer batch = new BatchSummarizer().setAttributes(attributes).setOutlierColumn("outlier");
        batch.process(df.filter("time", (double t) -> t >= eventIdx && t < eventEndIdx));
        Explanation batchResult = batch.getResults();

        /* Code to initialize incremental summarizer */
        IncrementalSummarizer summarizer = new IncrementalSummarizer(windowSize / slideSize);
        summarizer.setOutlierColumn("outlier").setAttributes(attributes);
        /* End */

        double startTime = 0.0;
        while (startTime < n) {
            double endTime = startTime + slideSize;
            double ls = startTime;
            DataFrame curBatch = df.filter("time", (double t) -> t >= ls && t < endTime);

            /* Code to process incremental summarizer on a minibatch */
            summarizer.process(curBatch);
            Explanation explanation = summarizer.getResults();
            /* End */

            assert (explanation.getNumInliers() + explanation.getNumOutliers() == Math.min(windowSize, endTime));

            if (startTime < eventIdx) { // No results before event time
                assert (explanation.getItemsets().size() == 0);
            } else {
                // Combination:
                //    make sure that the known anomalous attribute combination has the highest risk ratio
                List<AttributeSet> comboAttributes = explanation.getItemsets();
                Collections.sort(comboAttributes,
                        (a, b)->(new Double(b.getRatioToInliers()).compareTo(new Double(a.getRatioToInliers()))));
                assert(comboAttributes.get(0).getItems().values().containsAll(buggyAttributeValues));

                // Check whether result from the incremental summarizer in the first window agrees with
                // results from the batch summarizer for the event
                if (endTime == eventEndIdx) {
                    for (AttributeSet expected : batchResult.getItemsets()) {
                        if (expected.getItems().values().containsAll(buggyAttributeValues)) {
                            assert(expected.getNumRecords() == comboAttributes.get(0).getNumRecords());
                            break;
                        }
                    }
                }
            }
            startTime = endTime;

        }
    }

    @Test
    public void testDetectEndingEvent() {
        // Prepare data set
        int n = 12000;
        int k = 2;
        int C = 5;
        int d = 5;
        double p = 0.005;
        int eventIdx = 2000;
        int eventEndIdx = 4000;
        int windowSize = 5000;
        int slideSize = 1000;

        List<String> attributes = getAttributes(d, false);
        List<String> buggyAttributeValues = getAttributes(k, true);
        DataFrame df = generateAnomalyDataset(n, k, C, d, p, eventIdx, eventEndIdx);

        // Summarizer with combinations
        IncrementalSummarizer summarizer = new IncrementalSummarizer(windowSize / slideSize);
        summarizer.setOutlierColumn("outlier").setAttributes(attributes);

        double startTime = 0.0;
        while (startTime < n) {
            double endTime = startTime + slideSize;
            double ls = startTime;
            DataFrame curBatch = df.filter("time", (double t) -> t >= ls && t < endTime);
            summarizer.process(curBatch);
            Explanation explanation = summarizer.getResults(10);

            if (startTime >= eventIdx && endTime < eventEndIdx + windowSize) {
                // Make sure that the known anomalous attribute combination has the highest risk ratio
                // before all event panes retire
                Collections.sort(explanation.getItemsets(),
                        (a, b)->(new Double(b.getRatioToInliers()).compareTo(new Double(a.getRatioToInliers()))));
                assert(explanation.getItemsets().get(0).getItems().values().containsAll(buggyAttributeValues));
            } else {
                // Make sure that there is no explanation before the event, and after all event panes retire
                assert(explanation.getItemsets().size() == 0);
            }
            startTime = endTime;
        }
    }


    @Test
    public void testAttributeCombo() {
        // Prepare data set
        int n = 16000;
        int k = 4;
        int C = 4;
        int d = 10;
        double p = 0.005;
        int eventIdx = 0;
        int eventEndIdx = 16000;
        int windowSize = 4000;
        int slideSize = 4000;

        List<String> attributes = getAttributes(d, false);
        List<String> buggyAttributeValues = getAttributes(k, true);
        DataFrame df = generateAnomalyDataset(n, k, C, d, p, eventIdx, eventEndIdx);

        // Summarizer with combinations
        IncrementalSummarizer summarizer = new IncrementalSummarizer(windowSize / slideSize);
        summarizer.setOutlierColumn("outlier").setAttributes(attributes);

        double startTime = 0.0;
        while (startTime < n) {
            double endTime = startTime + slideSize;
            double ls = startTime;
            DataFrame curBatch = df.filter("time", (double t) -> t >= ls && t < endTime);
            summarizer.process(curBatch);
            Explanation explanation = summarizer.getResults(10);
            Collections.sort(explanation.getItemsets(),
                    (a, b)->(new Double(b.getRatioToInliers()).compareTo(new Double(a.getRatioToInliers()))));
            assert(explanation.getItemsets().get(0).getItems().values().containsAll(buggyAttributeValues));
            startTime = endTime;
        }
    }
}
