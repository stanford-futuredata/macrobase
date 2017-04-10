package edu.stanford.futuredata.macrobase;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.MovingAverage;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
        BatchSummarizer bs = new BatchSummarizer();
        bs.setAttributes(attributes);
        bs.setOutlierColumn("outlier");
        bs.setUseAttributeCombinations(true);
        bs.process(df);
        Explanation e = bs.getResults();
//        System.out.println(e);

        int slideSize = 600;
        // TODO: replace moving average with summarization
        MovingAverage ma = new MovingAverage("outlier", 3);
        double startTime = 0.0;
        while (startTime < n) {
            double endTime = startTime + slideSize;
            double ls = startTime;
            DataFrame curBatch = df.filter("time", (double t) -> t >= ls && t < endTime);
            ma.process(curBatch);
//            System.out.println(ma.getResults());
            startTime = endTime;
        }
    }
}
