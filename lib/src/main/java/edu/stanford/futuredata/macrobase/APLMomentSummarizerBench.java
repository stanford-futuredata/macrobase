package edu.stanford.futuredata.macrobase;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLMomentSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLOutlierSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.metrics.EstimatedSupportMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;

import java.io.IOException;
import java.util.*;

public class APLMomentSummarizerBench {
    double minSupport = 0.1;
    double percentile = 1.0;
    String outlierColumn = "outliers1";
    int numWarmupTrials;
    int numTrials;
    int numMoments;
    String oracleCubeFilename;
    String momentCubeFilename;
    boolean doContainment;
    List<String> attributes;
    boolean verbose;

    public APLMomentSummarizerBench(String confFile) throws IOException {
        RunConfig conf = RunConfig.fromJsonFile(confFile);
        minSupport = conf.get("minSupport");
        percentile = conf.get("percentile");
        outlierColumn = conf.get("outlierColumn");
        numWarmupTrials = conf.get("numWarmupTrials", 10);
        numTrials = conf.get("numTrials", 10);
        numMoments = conf.get("numMoments", 8);
        oracleCubeFilename = conf.get("oracleCubeFilename");
        momentCubeFilename = conf.get("momentCubeFilename");
        doContainment = conf.get("doContainment", false);
        attributes = conf.get("attributes");
        verbose = conf.get("verbose", false);
    }

    public static void main(String[] args) throws Exception {
        String confFile = args[0];
        APLMomentSummarizerBench bench = new APLMomentSummarizerBench(confFile);
        bench.run();
    }

    public void run() throws Exception {
        System.out.format("minSupport: %f, percentile: %f\n\n", minSupport, percentile);
        testOracleOrder3();
        testCubeOrder3(true);
        testCubeOrder3(false);
    }

    public void testOracleOrder3() throws Exception {
        List<String> requiredColumns = new ArrayList<>(attributes);
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put("count", Schema.ColType.DOUBLE);
        colTypes.put(outlierColumn, Schema.ColType.DOUBLE);
        requiredColumns.add("count");
        requiredColumns.add(outlierColumn);
        CSVDataFrameParser loader = new CSVDataFrameParser(oracleCubeFilename, requiredColumns);
        loader.setColumnTypes(colTypes);
        DataFrame df = loader.load();

        APLOutlierSummarizer summ = new APLOutlierSummarizer();
        summ.setCountColumn("count");
        summ.setOutlierColumn(outlierColumn);
        summ.setMinSupport(minSupport);
        summ.setMinRatioMetric(10.0);
        summ.setAttributes(attributes);
        summ.setDoContainment(doContainment);
        summ.onlyUseSupport(true);
        for (int i = 0; i < numWarmupTrials; i++) {
            summ.process(df);
        }
        long start = System.nanoTime();
        for (int i = 0; i < numTrials; i++) {
            summ.process(df);
        }
        long timeElapsed = System.nanoTime() - start;
        System.out.format("Oracle time: %g\n", timeElapsed / (1.e9 * numTrials));
        APLExplanation e = summ.getResults();
        System.out.format("Num results: %d\n\n", e.getResults().size());
        if (verbose) {
            System.out.println(e.prettyPrint());
        }
    }

    public void testCubeOrder3(boolean useCascade) throws Exception {
        List<String> requiredColumns = new ArrayList<>(attributes);
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        List<String> momentColumns = new ArrayList<>();
        for (int i = 0; i <= numMoments; i++) {
            colTypes.put("m" + i, Schema.ColType.DOUBLE);
            requiredColumns.add("m" + i);
            momentColumns.add("m" + i);
        }
        colTypes.put("min", Schema.ColType.DOUBLE);
        colTypes.put("max", Schema.ColType.DOUBLE);
        requiredColumns.add("min");
        requiredColumns.add("max");
        CSVDataFrameParser loader = new CSVDataFrameParser(momentCubeFilename, requiredColumns);
        loader.setColumnTypes(colTypes);
        DataFrame df = loader.load();

        APLMomentSummarizer summ = new APLMomentSummarizer();
        summ.setMinSupport(minSupport);
        summ.setMinRatioMetric(10.0);
        summ.setAttributes(attributes);
        summ.setMinColumn("min");
        summ.setMaxColumn("max");
        summ.setMomentColumns(momentColumns);
        summ.setPercentile(percentile);
        summ.setCascade(useCascade);
        summ.setDoContainment(doContainment);
        for (int i = 0; i < numWarmupTrials; i++) {
            summ.process(df);
        }
        long start = System.nanoTime();
        for (int i = 0; i < numTrials; i++) {
            summ.process(df);
        }
        long timeElapsed = System.nanoTime() - start;
        System.out.format("%s time: %g\n", useCascade ? "Cascade" : "Maxent", timeElapsed / (1.e9 * numTrials));
        if (useCascade) {
            EstimatedSupportMetric metric = (EstimatedSupportMetric)summ.qualityMetricList.get(0);
            int prunedByNaive = metric.numEnterCascade - metric.numAfterNaiveCheck;
            int prunedByMarkov = metric.numAfterNaiveCheck - metric.numAfterMarkovBound;
            int prunedByMoments = metric.numAfterMarkovBound - metric.numAfterMomentBound;
            System.out.format("Cascade PTR\n\t" +
                            "Entered cascade: %d\n\t" +
                            "Pruned by naive checks: %d (%f)\n\t" +
                            "Pruned by Markov bounds: %d (%f)\n\t" +
                            "Pruned by moment bounds: %d (%f)\n\t" +
                            "Reached maxent: %d (%f)\n",
                    metric.numEnterCascade,
                    prunedByNaive, prunedByNaive / (double)metric.numEnterCascade,
                    prunedByMarkov, prunedByMarkov / (double)metric.numEnterCascade,
                    prunedByMoments, prunedByMoments / (double)metric.numEnterCascade,
                    metric.numAfterMomentBound, metric.numAfterMomentBound / (double)metric.numEnterCascade);
        }
        APLExplanation e = summ.getResults();
        System.out.format("Num results: %d\n\n", e.getResults().size());
        if (verbose) {
            System.out.println(e.prettyPrint());
        }
    }
}