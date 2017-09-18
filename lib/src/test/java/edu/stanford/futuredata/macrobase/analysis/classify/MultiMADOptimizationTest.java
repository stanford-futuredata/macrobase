package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.junit.Before;
import org.junit.Test;

import java.lang.Math;
import java.lang.Double;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiMADOptimizationTest {
    private DataFrame df;
    private String[] columnNames;
    private String attributeName = "A9";
    private static List<Double> trueMedians;
    private static List<Double> trueMADs;
    private static List<Integer> trueOutliers;
    private int numTrials = 10;  // TODO: change this
    private double percentOutliers = 0.1;
    private long startTime = 0;
    private long estimatedTime = 0;
    private MultiMADClassifierDebug mad;

    @Before
    public void setUp() throws Exception {
//         Map<String, Schema.ColType> schema = new HashMap<>();
//         columnNames = new String[27];
//         for (int i = 0; i < 27; i++) {
//             columnNames[i] = "f" + String.valueOf(i);
//             schema.put(columnNames[i], Schema.ColType.DOUBLE);
//         }
//         DataFrameLoader loader = new CSVDataFrameLoader(
//                 "src/test/resources/hepmass100k.csv"
//         ).setColumnTypes(schema);
//         df = loader.load();

        Map<String, Schema.ColType> schema = new HashMap<>();
        columnNames = new String[9];
        for (int i = 0; i < 9; i++) {
            columnNames[i] = "A" + String.valueOf(i);
            schema.put(columnNames[i], Schema.ColType.DOUBLE);
        }
        schema.put("A9", Schema.ColType.DOUBLE);
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/shuttle.csv"
        ).setColumnTypes(schema);
        df = loader.load();
    }

    @Test
    public void testBenchmark() throws Exception {
        // double[] metrics = df.getDoubleColumnByName(columnNames[24]);
        // Arrays.sort(metrics);

        startTime = System.currentTimeMillis();

        mad = new MultiMADClassifierDebug(attributeName, columnNames)
                .setPercentile(percentOutliers);
        for (int i = 0; i < numTrials; i++) {
            mad.process(df);
        }

        // int len = metrics.length;
        // System.out.format("min: %f, 25: %f, median: %f, 75: %f, max: %f, MAD: %f\n",
        //     metrics[0], metrics[len/4], metrics[len/2], metrics[len*3/4],
        //     metrics[metrics.length-1], mad.getMADs().get(24));

        estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("Unoptimized time elapsed: %d ms\n", estimatedTime - mad.getOtherTime());
        System.out.format("train: %d ms, score: %d ms, sampling: %d ms, bootstrap (not counted): %d ms\n",
            mad.getTrainTime(), mad.getScoreTime(), mad.getSamplingTime(), mad.getOtherTime());
        System.out.println("");

        // trueOutliers = mad.getOutlierIndices();

         trueMedians = mad.getMedians();
         trueMADs = mad.getMADs();
    }

     @Test
     public void testSamplingOptimization() throws Exception {
         samplingRun(2);
         samplingRun(10);
         samplingRun(100);
     }

    public void samplingRun(int samplingRate) {
        startTime = System.currentTimeMillis();

        mad = new MultiMADClassifierDebug(attributeName, columnNames)
                .setPercentile(percentOutliers)
                .setSamplingRate(samplingRate);
        for (int i = 0; i < numTrials; i++) {
            mad.process(df);
        }

        estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("Sampling (1/%d of elements) time elapsed: %d ms\n", samplingRate, estimatedTime - mad.getOtherTime());
        System.out.format("train: %d ms, score: %d ms, sampling: %d ms, boostrap (not counted): %d ms\n",
            mad.getTrainTime(), mad.getScoreTime(), mad.getSamplingTime(), mad.getOtherTime());

        // // True positive rate
        // List<Integer> outliers = mad.getOutlierIndices();
        // int numOutliersFound = outliers.size();
        // outliers.retainAll(trueOutliers);
        // int numTrueOutliersFound = outliers.size();
        // System.out.format("Found %d of %d outliers (%f), with %d false positives\n",
        //     numTrueOutliersFound, trueOutliers.size(),
        //     (double)numTrueOutliersFound / trueOutliers.size(),
        //     numOutliersFound - numTrueOutliersFound);

        if (mad.doBootstrap) {
            List<Double> medians = mad.getMedians();
            List<Double> MADs = mad.getMADs();
            List<Double> upperBoundsMedian = mad.upperBoundsMedian;
            List<Double> lowerBoundsMedian = mad.lowerBoundsMedian;
            List<Double> upperBoundsMAD = mad.upperBoundsMAD;
            List<Double> lowerBoundsMAD = mad.lowerBoundsMAD;
            double med_sum = 0;
            double mad_sum = 0;
            double med_err_sum = 0;
            double mad_err_sum = 0;
            int num_metrics = columnNames.length;
            for (int i = 0; i < num_metrics; i++) {
                double medianError = Math.abs(medians.get(i) - trueMedians.get(i)) / trueMADs.get(i);
                double MADError = Math.abs(MADs.get(i) - trueMADs.get(i)) / trueMADs.get(i);
                double medianCItoMAD = (upperBoundsMedian.get(i) - lowerBoundsMedian.get(i)) / trueMADs.get(i);
                double madCItoMAD = (upperBoundsMAD.get(i) - lowerBoundsMAD.get(i)) / trueMADs.get(i);
                med_sum += medianCItoMAD;
                mad_sum += madCItoMAD;
                med_err_sum += medianError;
                mad_err_sum += MADError;
                // System.out.format("Column %d: median %f [%f, %f], ratio: %f, raw err: %f\n",
                //     i, medians.get(i), lowerBoundsMedian.get(i), upperBoundsMedian.get(i), medianCItoMAD, medianError);
                // System.out.format("Column %d: MAD %f [%f, %f], ratio: %f, raw err: %f\n",
                //     i, MADs.get(i), lowerBoundsMAD.get(i), upperBoundsMAD.get(i), madCItoMAD, medianError);
            }
            System.out.format("Avg median ratio: %f, avg MAD ratio: %f, med error: %f, MAD error: %f\n",
                    med_sum / (double) num_metrics,
                    mad_sum / (double) num_metrics,
                    med_err_sum / (double) num_metrics,
                    mad_err_sum / (double) num_metrics
            );
            System.out.println("");
        }
    }
}