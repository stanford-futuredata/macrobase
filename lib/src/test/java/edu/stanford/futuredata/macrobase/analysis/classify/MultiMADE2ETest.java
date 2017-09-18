package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.IncrementalSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import edu.stanford.futuredata.macrobase.operator.WindowedOperator;
import org.junit.Before;
import org.junit.Test;

import java.lang.Math;
import java.lang.Double;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiMADE2ETest {
    private DataFrame df;
    private DataFrame df_classified;
    private String[] columnNames;
    private String[] metricColumnNames;
    private String[] attributeColumnNames;
    private String[] metricNames;
    private String attributeName = "A9";
    private static List<Double> trueMedians;
    private static List<Double> trueMADs;
    private static List<Integer> trueOutliers;
    private int numTrials = 100;  // TODO: change this
    private double percentOutliers = 1;
    private long startTime = 0;
    private long estimatedTime = 0;
    private MultiMADClassifierE2E mad;

    // @Test
    // public void testE2ESynthetic() throws Exception {
    //     Map<String, Schema.ColType> schema = new HashMap<>();
    //     metricColumnNames = new String[3];
    //     attributeColumnNames = new String[6];
    //     // attributeColumnNames[0] = "time";
    //     schema.put("time", Schema.ColType.STRING);
    //     attributeColumnNames[0] = "cache_server";
    //     schema.put("cache_server", Schema.ColType.STRING);
    //     metricColumnNames[0] = "cpu_usage";
    //     schema.put("cpu_usage", Schema.ColType.DOUBLE);
    //     attributeColumnNames[1] = "android_version";
    //     schema.put("android_version", Schema.ColType.STRING);
    //     attributeColumnNames[2] = "hardware_model";
    //     schema.put("hardware_model", Schema.ColType.STRING);
    //     attributeColumnNames[3] = "origin_country";
    //     schema.put("origin_country", Schema.ColType.STRING);
    //     attributeColumnNames[4] = "power_saving";
    //     schema.put("power_saving", Schema.ColType.STRING);
    //     metricColumnNames[1] = "response_latency";
    //     schema.put("response_latency", Schema.ColType.DOUBLE);
    //     metricColumnNames[2] = "app_version";
    //     schema.put("app_version", Schema.ColType.DOUBLE);
    //     attributeColumnNames[5] = "network_type";
    //     schema.put("network_type", Schema.ColType.STRING);
    //     DataFrameLoader loader = new CSVDataFrameLoader(
    //             "src/test/resources/data.csv"
    //     ).setColumnTypes(schema);
    //     df = loader.load();

    //     addNoise(3);

    //     attributeName = attributeColumnNames[1];
    //     // metricNames = new String[]{metricColumnNames[0], metricColumnNames[1]};
    //     metricNames = metricColumnNames;   
    //     runTests();     
    // }

    @Test
    public void testE2EShuttle() throws Exception {
        // Map<String, Schema.ColType> schema = new HashMap<>();
        // columnNames = new String[27];
        // for (int i = 0; i < 27; i++) {
        //     columnNames[i] = "f" + String.valueOf(i);
        //     schema.put(columnNames[i], Schema.ColType.DOUBLE);
        // }
        // DataFrameLoader loader = new CSVDataFrameLoader(
        //         "src/test/resources/hepmass100k.csv"
        // ).setColumnTypes(schema);
        // df = loader.load();

        Map<String, Schema.ColType> schema = new HashMap<>();
        metricColumnNames = new String[9];
        for (int i = 0; i < 9; i++) {
            metricColumnNames[i] = "A" + String.valueOf(i);
            schema.put(metricColumnNames[i], Schema.ColType.DOUBLE);
        }
        schema.put("A9", Schema.ColType.STRING);
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/shuttle.csv"
        ).setColumnTypes(schema);
        df = loader.load();

        // addNoise(9);

        metricNames = metricColumnNames;

        runTests();
    }

    public void runTests() throws Exception {
        // MultiMAD only classification (no group by)
         startTime = System.currentTimeMillis();

         mad = new MultiMADClassifierE2E(attributeName, metricNames)
                  .setPercentile(percentOutliers);
//                 .setZscore(1.64485);
         for (int i = 0; i < numTrials; i++) {
             mad.process(df);
         }

         estimatedTime = System.currentTimeMillis() - startTime;
         System.out.format("Only classification time elapsed: %d ms\n", estimatedTime);

        // Percentile Group By E2E
         startTime = System.currentTimeMillis();

         mad = new MultiMADClassifierE2E(attributeName, metricNames)
                 .setPercentile(percentOutliers)
                 .setDoGroupBy(true)
                 .setUsePercentile(true);
         for (int i = 0; i < numTrials; i++) {
             mad.process(df);
         }

         estimatedTime = System.currentTimeMillis() - startTime;
         System.out.format("Percentile E2E w/group by time elapsed: %d ms\n", estimatedTime);
//         System.out.format("\t%s\n", Arrays.toString(mad.features.entrySet().toArray()));
//         System.out.format("\t%s\n", Arrays.toString(mad.ratios.entrySet().toArray()));

        // MultiMAD Group By E2E
         startTime = System.currentTimeMillis();

         mad = new MultiMADClassifierE2E(attributeName, metricNames)
                 .setPercentile(percentOutliers)
                 .setDoGroupBy(true);
         for (int i = 0; i < numTrials; i++) {
             mad.process(df);
         }

         estimatedTime = System.currentTimeMillis() - startTime;
         System.out.format("Multimad E2E w/group by time elapsed: %d ms\n", estimatedTime);
//         System.out.format("\t%s\n", Arrays.toString(mad.features.entrySet().toArray()));
//         System.out.format("\t%s\n", Arrays.toString(mad.ratios.entrySet().toArray()));

        // MultiMAD Fast Group By E2E
        startTime = System.currentTimeMillis();

        mad = new MultiMADClassifierE2E(attributeName, metricNames)
                .setPercentile(percentOutliers)
                .setDoGroupByFast(true);
        for (int i = 0; i < numTrials; i++) {
            mad.process(df);
        }

        estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("Multimad E2E w/fast group by time elapsed: %d ms\n", estimatedTime);
//        System.out.format("\t%s\n", Arrays.toString(mad.features.entrySet().toArray()));
//        System.out.format("\t%s\n", Arrays.toString(mad.ratios.entrySet().toArray()));
//        fastGroupByMeasures();

        // Univariate feature selectors
         startTime = System.currentTimeMillis();

         FeatureSelector fs = new FeatureSelector(attributeName, metricNames);
         List<Double> feature_ranks = new ArrayList<Double>();
         for (int i = 0; i < numTrials; i++) {
             feature_ranks = fs.process_anova(df);
         }

         estimatedTime = System.currentTimeMillis() - startTime;
         System.out.format("Feature selection time elapsed (with Apache commons anova): %d ms\n", estimatedTime);
//         System.out.format("\t%s\n", Arrays.toString(feature_ranks.toArray()));

         startTime = System.currentTimeMillis();

         double[] feature_ranks2 = new double[]{};
         for (int i = 0; i < numTrials; i++) {
             feature_ranks2 = fs.process_ssr(df);
         }

         estimatedTime = System.currentTimeMillis() - startTime;
         System.out.format("Feature selection time elapsed (with Smile SSR): %d ms\n", estimatedTime);
//         System.out.format("\t%s\n", Arrays.toString(feature_ranks2));

//         // Logistic Regression feature selector
//         startTime = System.currentTimeMillis();
//
//         LRClassifier lr = new LRClassifier(attributeName, metricNames);
//         for (int i = 0; i < numTrials; i++) {
//             lr.process(df);
//         }
//
//         estimatedTime = System.currentTimeMillis() - startTime;
//         System.out.format("Logistic regression time elapsed: %d ms\n", estimatedTime);
//         System.out.format("\t%s\n", Arrays.toString(lr.topFeatures.toArray()));
//         // System.out.format("%s\n", mad.explanations.get(0).prettyPrint());

        // Not sure what this next part is
//         startTime = System.currentTimeMillis();

        // mad = new MultiMADClassifierE2E(attributeName, metricNames)
        //         .setPercentile(percentOutliers);
        // for (int i = 0; i < numTrials; i++) {
        //     mad.process(df);
        //     BatchSummarizer summarizer = new BatchSummarizer()
        //         .setAttributes(Arrays.asList(attributeName))
        //         .setUseAttributeCombinations(false);
        //     for (int j = 0; j < metricNames.length; j++) {
        //         summarizer.setOutlierColumn(metricNames[j] + "_OUTLIER");
        //         summarizer.process(mad.getResults());
        //         // if (j==4) {
        //         //     System.out.format("%s\n", summarizer.getResults().prettyPrint());
        //         // }
        //     }
        // }

        // estimatedTime = System.currentTimeMillis() - startTime;
        // System.out.format("Benchmark time elapsed: %d ms\n", estimatedTime);

        // percentileSummarizer();
    }

    public void fastGroupByMeasures() throws Exception {
        List<String> lines = new ArrayList<String>();
        int[] trial_sizes = {10, 100, 1000, 10000, 20000};

        for (int n_trials : trial_sizes) {
            long time = fastGroupBy(n_trials);
            lines.add(String.valueOf(n_trials) + ", " + String.valueOf(time));
        }

        Path file = Paths.get("multimad.csv");
        Files.write(file, lines, Charset.forName("UTF-8"));
    }

    public long fastGroupBy() {
        return fastGroupBy(numTrials);
    }

    public long fastGroupBy(int n_trials) {
        startTime = System.currentTimeMillis();

        mad = new MultiMADClassifierE2E(attributeName, metricNames)
                .setPercentile(percentOutliers)
                .setDoGroupByFast(true);
        for (int i = 0; i < n_trials; i++) {
            mad.process(df);
        }

        estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("E2E fast time elapsed: %d ms\n", estimatedTime);
        System.out.format("\t%s\n", Arrays.toString(mad.features.entrySet().toArray()));
        System.out.format("\t%s\n", Arrays.toString(mad.ratios.entrySet().toArray()));
        // System.out.format("%s\n", mad.explanations.get(0).prettyPrint());

        return estimatedTime;
    }

    public void percentileSummarizer() {
        startTime = System.currentTimeMillis();

        for (String name : metricNames) {
            PercentileClassifier pc = new PercentileClassifier(name)
                .setPercentile(1.0)
                .setIncludeHigh(true)
                .setIncludeLow(false);
            pc.process(df);
            DataFrame df_classified = pc.getResults();

            BatchSummarizer summ = new BatchSummarizer()
                    .setAttributes(Arrays.asList(attributeName))
                    .setUseAttributeCombinations(false)
                    .setMinSupport(0.002)
                    .setMinRiskRatio(1);
            summ.process(df_classified);
            Explanation results = summ.getResults();

            System.out.format(results.prettyPrint());
        }
        
        estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("Percentile + batch summarizer time elapsed: %d ms\n", estimatedTime);
    }

    public void addNoise(int numColumns) {
        Random rand = new Random();
        String[] newMetricColumnNames = new String[metricColumnNames.length + numColumns];
        System.arraycopy(metricColumnNames, 0, newMetricColumnNames, 0, metricColumnNames.length);
        for (int i = 0; i < numColumns; i++) {
            double[] newCol = new double[df.getNumRows()];
            for (int j = 0; j < df.getNumRows(); j++) {
                newCol[j] = rand.nextGaussian();
            }
            String name = "noise_" + String.valueOf(i);
            df.addDoubleColumn(name, newCol);
            newMetricColumnNames[i + metricColumnNames.length] = name;
        }
        metricColumnNames = newMetricColumnNames;
    }
}