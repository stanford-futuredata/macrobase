package edu.stanford.futuredata.macrobase.integration;

import edu.stanford.futuredata.macrobase.StreamingSummarizationTest;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.IncrementalSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGrowthSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.result.FPGAttributeSet;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.WindowedOperator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Compare the performance of sliding window summarization with repeated batch summarization.
 * The incremental sliding window operator should be noticeably faster.
 */
public class StreamingSummarizationBenchmark {
    public static void testWindowedPerformance() throws Exception {
        // Increase these numbers for more rigorous, slower performance testing
        int n = 100000;
        int k = 3;
        int C = 4;
        int d = 10;
        double p = 0.005;
        int eventIdx = 40000;
        int eventEndIdx = 100000;
        int windowSize = 50000;
        int slideSize = 1000;

        DataFrame df = StreamingSummarizationTest.generateAnomalyDataset(n, k, C, d, p, eventIdx, eventEndIdx);
        List<String> attributes = StreamingSummarizationTest.getAttributes(d, false);
        List<String> buggyAttributeValues = StreamingSummarizationTest.getAttributes(k, true);

        IncrementalSummarizer outlierSummarizer = new IncrementalSummarizer();
        outlierSummarizer.setAttributes(attributes);
        outlierSummarizer.setOutlierColumn("outlier");
        outlierSummarizer.setMinSupport(.3);
        WindowedOperator<FPGExplanation> windowedSummarizer = new WindowedOperator<>(outlierSummarizer);
        windowedSummarizer.setWindowLength(windowSize);
        windowedSummarizer.setTimeColumn("time");
        windowedSummarizer.setSlideLength(slideSize);
        windowedSummarizer.initialize();

        FPGrowthSummarizer bsumm = new FPGrowthSummarizer();
        bsumm.setAttributes(attributes);
        bsumm.setOutlierColumn("outlier");
        bsumm.setMinSupport(.3);

        int miniBatchSize = slideSize;
        double totalStreamingTime = 0.0;
        double totalBatchTime = 0.0;

        double startTime = 0.0;
        int nRows = df.getNumRows();
        while (startTime < nRows) {
            double endTime = startTime + miniBatchSize;
            double ls = startTime;
            DataFrame curBatch = df.filter(
                    "time",
                    (double t) -> t >= ls && t < endTime
            );
            long timerStart = System.currentTimeMillis();
            windowedSummarizer.process(curBatch);
            windowedSummarizer.flushBuffer();
            if (endTime >= windowSize) {
                FPGExplanation curExplanation = windowedSummarizer
                        .getResults()
                        .prune();
                long timerElapsed = System.currentTimeMillis() - timerStart;
                totalStreamingTime += timerElapsed;

                DataFrame curWindow = df.filter(
                        "time",
                        (double t) -> t >= (endTime - windowSize) && t < endTime
                );
                timerStart = System.currentTimeMillis();
                bsumm.process(curWindow);
                FPGExplanation batchExplanation = bsumm.getResults().prune();
                timerElapsed = System.currentTimeMillis() - timerStart;
                totalBatchTime += timerElapsed;

                //  make sure that the known anomalous attribute combination has the highest risk ratio
                if (curExplanation.getItemsets().size() > 0) {
                    FPGAttributeSet streamTopRankedExplanation = curExplanation.getItemsets().get(0);
                    FPGAttributeSet batchTopRankedExplanation = batchExplanation.getItemsets().get(0);
                    assertTrue(streamTopRankedExplanation.getItems().values().containsAll(buggyAttributeValues));
                    assertTrue(batchTopRankedExplanation.getItems().values().containsAll(buggyAttributeValues));
                }
            } else {
                long timerElapsed = System.currentTimeMillis() - timerStart;
                totalStreamingTime += timerElapsed;
            }
            startTime = endTime;
        }

        System.out.println(String.format("window size: %d, slide size: %d", windowSize, slideSize));
        System.out.println("Streaming Time: "+totalStreamingTime);
        System.out.println("Batch Time: "+totalBatchTime);
    }

    public static void main(String[] args) throws Exception {
        testWindowedPerformance();
    }
}
