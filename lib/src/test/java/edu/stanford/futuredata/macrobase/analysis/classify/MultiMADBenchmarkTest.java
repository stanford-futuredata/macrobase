package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiMADBenchmarkTest {
    private DataFrame df;

    @Test
    public void testSampleBenchmark() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        schema.put("usage", Schema.ColType.DOUBLE);
        schema.put("latency", Schema.ColType.DOUBLE);
        schema.put("location", Schema.ColType.STRING);
        schema.put("version", Schema.ColType.STRING);
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/sample.csv"
        ).setColumnTypes(schema);
        df = loader.load();

        long startTime = System.currentTimeMillis();

        MultiMADClassifier mad = new MultiMADClassifier("usage", "latency")
                .setPercentile(5);
        mad.process(df);
        DataFrame df_classified = mad.getResults();

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("Sample benchmark time elapsed: %d ms\n", estimatedTime);
    }

    @Test
    public void testSampleBenchmarkWithDuplicateColumns() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        schema.put("usage", Schema.ColType.DOUBLE);
        schema.put("latency", Schema.ColType.DOUBLE);
        schema.put("location", Schema.ColType.STRING);
        schema.put("version", Schema.ColType.STRING);
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/sample.csv"
        ).setColumnTypes(schema);
        df = loader.load();

        // Duplicate columns
        String[] madColumns = new String[1000];
        double[] dup = df.getDoubleColumnByName("usage");
        for (int i = 0; i < 1000; i++) {
            df.addDoubleColumn("u" + String.valueOf(i), dup);
            madColumns[i] = "u" + String.valueOf(i);
        }

        long startTime = System.currentTimeMillis();

        MultiMADClassifier mad = new MultiMADClassifier(madColumns)
                .setPercentile(5);
        mad.process(df);
        DataFrame df_classified = mad.getResults();

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("Sample (dup) time elapsed: %d ms\n", estimatedTime);
    }

    @Test
    public void testShuttleBenchmark() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        String[] columnNames = new String[10];
        for (int i = 0; i < 10; i++) {
            columnNames[i] = "A" + String.valueOf(i);
            schema.put(columnNames[i], Schema.ColType.DOUBLE);
        }
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/shuttle.csv"
        ).setColumnTypes(schema);
        df = loader.load();

        long startTime = System.currentTimeMillis();

        MultiMADClassifier mad = new MultiMADClassifier(columnNames)
                .setPercentile(5);
        mad.process(df);
        DataFrame df_classified = mad.getResults();

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("Shuttle benchmark time elapsed: %d ms\n", estimatedTime);
    }

    @Test
    public void testHepmassBenchmark() throws Exception {
        Map<String, Schema.ColType> schema = new HashMap<>();
        String[] columnNames = new String[27];
        for (int i = 0; i < 27; i++) {
            columnNames[i] = "f" + String.valueOf(i);
            schema.put(columnNames[i], Schema.ColType.DOUBLE);
        }
        DataFrameLoader loader = new CSVDataFrameLoader(
                "src/test/resources/hepmass1M.csv"
        ).setColumnTypes(schema);
        df = loader.load();

        long startTime = System.currentTimeMillis();

        MultiMADClassifier mad = new MultiMADClassifier(columnNames)
                .setPercentile(5);
        mad.process(df);
        DataFrame df_classified = mad.getResults();

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.format("HEPMASS benchmark time elapsed: %d ms\n", estimatedTime);
    }
}