package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiMADBenchmarkTest {
    private DataFrame df;
    private DataFrame df_classified;
    private String[] columnNames;
    private String[] testColumnNames;
    private List<String> lines;

    // @Test
    // public void testSampleBenchmark() throws Exception {
    //     Map<String, Schema.ColType> schema = new HashMap<>();
    //     schema.put("usage", Schema.ColType.DOUBLE);
    //     schema.put("latency", Schema.ColType.DOUBLE);
    //     schema.put("location", Schema.ColType.STRING);
    //     schema.put("version", Schema.ColType.STRING);
    //     DataFrameLoader loader = new CSVDataFrameLoader(
    //             "src/test/resources/sample.csv"
    //     ).setColumnTypes(schema);
    //     df = loader.load();

    //     long startTime = System.currentTimeMillis();

    //     MultiMADClassifier mad = new MultiMADClassifier("usage", "latency")
    //             .setPercentile(5);
    //     mad.process(df);
    //     DataFrame df_classified = mad.getResults();

    //     long estimatedTime = System.currentTimeMillis() - startTime;
    //     System.out.format("Sample benchmark time elapsed: %d ms\n", estimatedTime);
    // }

    // @Test
    // public void testSampleBenchmarkWithDuplicateColumns() throws Exception {
    //     Map<String, Schema.ColType> schema = new HashMap<>();
    //     schema.put("usage", Schema.ColType.DOUBLE);
    //     schema.put("latency", Schema.ColType.DOUBLE);
    //     schema.put("location", Schema.ColType.STRING);
    //     schema.put("version", Schema.ColType.STRING);
    //     DataFrameLoader loader = new CSVDataFrameLoader(
    //             "src/test/resources/sample.csv"
    //     ).setColumnTypes(schema);
    //     df = loader.load();

    //     // Duplicate columns
    //     String[] madColumns = new String[1000];
    //     double[] dup = df.getDoubleColumnByName("usage");
    //     for (int i = 0; i < 1000; i++) {
    //         df.addDoubleColumn("u" + String.valueOf(i), dup);
    //         madColumns[i] = "u" + String.valueOf(i);
    //     }

    //     long startTime = System.currentTimeMillis();

    //     MultiMADClassifier mad = new MultiMADClassifier(madColumns)
    //             .setPercentile(5);
    //     mad.process(df);
    //     DataFrame df_classified = mad.getResults();

    //     long estimatedTime = System.currentTimeMillis() - startTime;
    //     System.out.format("Sample (dup) time elapsed: %d ms\n", estimatedTime);
    // }

    // @Test
    // public void testShuttleBenchmark() throws Exception {
    //     Map<String, Schema.ColType> schema = new HashMap<>();
    //     String[] columnNames = new String[10];
    //     for (int i = 0; i < 10; i++) {
    //         columnNames[i] = "A" + String.valueOf(i);
    //         schema.put(columnNames[i], Schema.ColType.DOUBLE);
    //     }
    //     DataFrameLoader loader = new CSVDataFrameLoader(
    //             "src/test/resources/shuttle.csv"
    //     ).setColumnTypes(schema);
    //     df = loader.load();

    //     long startTime = System.currentTimeMillis();

    //     MultiMADClassifier mad = new MultiMADClassifier(columnNames)
    //             .setPercentile(5);
    //     mad.process(df);
    //     DataFrame df_classified = mad.getResults();

    //     long estimatedTime = System.currentTimeMillis() - startTime;
    //     System.out.format("Shuttle benchmark time elapsed: %d ms\n", estimatedTime);
    // }

    @Test
    public void testHepmassBenchmark() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        columnNames = new String[27];
        for (int i = 0; i < 27; i++) {
            columnNames[i] = "f" + String.valueOf(i);
            schema.put(columnNames[i], Schema.ColType.DOUBLE);
        }
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/hepmass1M.csv"
        ).setColumnTypes(schema);
        df = loader.load();

        lines = new ArrayList<String>();

        int[] numColumns = {1, 5, 10, 20};

        for (int num : numColumns) {
            testColumnNames = new String[num];
            System.arraycopy(columnNames, 0, testColumnNames, 0, num);

            int numTrials = 100;

            System.out.println("HEPMASS");

            runPercentileClassifier(numTrials);

            runMultiMADClassifier(numTrials, 1.0);
            runMultiMADClassifier(numTrials, 0.1);
            runMultiMADClassifier(numTrials, 0.01);
        }

        Path file = Paths.get("benchmark.csv");
        Files.write(file, lines, Charset.forName("UTF-8"));
    }

    public void runPercentileClassifier(int numTrials) {
        long startTime = System.currentTimeMillis();

        for (String columnName : testColumnNames) {
            PercentileClassifier pc = new PercentileClassifier(columnName)
                    .setPercentile(5);
            for (int i = 0; i < numTrials; i++) {
                pc.process(df);
                df_classified = pc.getResults();
            }
        }

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("PercentileClassifier: %d ms\n", estimatedTime);
        lines.add("percentile_classifier, " + String.valueOf(testColumnNames.length) +
                ", " + String.valueOf(estimatedTime));
    }

    public void runMultiMADClassifier(int numTrials, double samplingRate) {
        long startTime = System.currentTimeMillis();

        MultiMADClassifier mad = new MultiMADClassifier(testColumnNames)
                .setPercentile(5)
                .setSamplingRate(samplingRate);
        for (int i = 0; i < numTrials; i++) {
            mad.process(df);
            df_classified = mad.getResults();
        }

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("MultiMADClassifier, sampling rate %f: %d ms\n",
            samplingRate, estimatedTime);
        lines.add("multimad_" + String.valueOf(samplingRate) + ", " +
            String.valueOf(testColumnNames.length) + ", " + String.valueOf(estimatedTime));
    }
}