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
    private static List<Double> trueMedians;
    private static List<Double> trueMADs;
    private int numTrials = 1000;

    @Before
    public void setUp() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        columnNames = new String[27];
        for (int i = 0; i < 27; i++) {
            columnNames[i] = "f" + String.valueOf(i);
            schema.put(columnNames[i], Schema.ColType.DOUBLE);
        }
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/hepmass100k.csv"
        ).setColumnTypes(schema);
        df = loader.load();
    }

    @Test
    public void testHepmassBenchmark() throws Exception {
        long startTime = System.currentTimeMillis();

        MultiMADClassifier mad = new MultiMADClassifier(columnNames)
                .setPercentile(5);
        for (int i = 0; i < numTrials; i++) {
            // mad = new MultiMADClassifier(columnNames)
            //     .setPercentile(5);
            mad.process(df);
            // DataFrame df_classified = mad.getResults();
        }

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("Unoptimized time elapsed: %d ms\n", estimatedTime);

        trueMedians = new ArrayList<Double>(mad.getMedians());
        trueMADs = new ArrayList<Double>(mad.getMADs());
    }

    @Test
    public void testSamplingOptimization() throws Exception {
        long startTime = System.currentTimeMillis();

        MultiMADClassifier mad = new MultiMADClassifier(columnNames)
                .setPercentile(5)
                .setSamplingRate(2);
        for (int i = 0; i < numTrials; i++) {
            // mad = new MultiMADClassifier(columnNames)
            //         .setPercentile(5)
            //         .setSamplingRate(2);
            mad.process(df);
            // DataFrame df_classified = mad.getResults();
        }

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("Sampling (2) time elapsed: %d ms\n", estimatedTime);

        List<Double> medians = mad.getMedians();
        List<Double> MADs = mad.getMADs();
        for (int i = 0; i < 27; i++) {
            double medianError = Math.abs(medians.get(i) - trueMedians.get(i)) / trueMADs.get(i);
            double MADError = Math.abs(MADs.get(i) - trueMADs.get(i)) / trueMADs.get(i);
            System.out.format("Column %d: median error %f, MAD error %f\n",
                i, medianError, MADError);
        }

        startTime = System.currentTimeMillis();

        mad = new MultiMADClassifier(columnNames)
                .setPercentile(5)
                .setSamplingRate(10);
        for (int i = 0; i < numTrials; i++) {
            // mad = new MultiMADClassifier(columnNames)
            //         .setPercentile(5)
            //         .setSamplingRate(10);
            mad.process(df);
            // DataFrame df_classified = mad.getResults();
        }

        estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("Sampling (10) time elapsed: %d ms\n", estimatedTime);

        medians = mad.getMedians();
        MADs = mad.getMADs();
        for (int i = 0; i < 27; i++) {
            double medianError = Math.abs(medians.get(i) - trueMedians.get(i)) / trueMADs.get(i);
            double MADError = Math.abs(MADs.get(i) - trueMADs.get(i)) / trueMADs.get(i);
            System.out.format("Column %d: median error %f, MAD error %f\n",
                i, medianError, MADError);
        }
    }
}