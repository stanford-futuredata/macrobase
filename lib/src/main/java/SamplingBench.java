import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PredicateClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.*;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGrowthSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import io.CSVOutput;
import org.apache.commons.math3.analysis.function.Exp;

import java.io.IOException;
import java.util.*;

public class SamplingBench {
    private String testName;
    private String fileName;

    private String metric;
    private double cutoff;
    private boolean pctileHigh;
    private boolean pctileLow;

    private List<String> attributes;
    private String ratioMetric;
    private double minSupport;
    private double minRiskRatio;

    private List<Double> sampleRates;
    private List<Double> outlierSampleFractions;
    private int numTrials;
    private int warmupTime;

    private boolean useBasic;
    private boolean simpleEncoding;

    private boolean verbose = false;
    private boolean calcError = false;
    private boolean appendTimeStamp = true;

    public SamplingBench(String confFile) throws IOException{
        RunConfig conf = RunConfig.fromJsonFile(confFile);
        testName = conf.get("testName");
        fileName = conf.get("fileName");

        metric = conf.get("metric");
        cutoff = conf.get("cutoff", 1.0);
        pctileHigh = conf.get("includeHi",true);
        pctileLow = conf.get("includeLo", true);

        attributes = conf.get("attributes");
        ratioMetric = conf.get("ratioMetric", "globalRatio");
        minRiskRatio = conf.get("minRatioMetric", 3.0);
        minSupport = conf.get("minSupport", 0.01);

        sampleRates = conf.get("sampleRates");
        outlierSampleFractions = conf.get("outlierSampleFractions");
        numTrials = conf.get("numTrials");
        warmupTime = conf.get("warmupTime", 5);

        useBasic = conf.get("basic", false);
        simpleEncoding = conf.get("simpleEncoding", false);

        verbose = conf.get("verbose", false);
        calcError = conf.get("calcError", false);
        appendTimeStamp = conf.get("appendTimeStamp", false);
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        String confFile = args[0];
        SamplingBench bench = new SamplingBench(confFile);

        List<Map<String, String>> results = bench.run();
        CSVOutput output = new CSVOutput();
        output.setAddTimeStamp(bench.appendTimeStamp);
        output.writeAllResults(results, bench.testName);
        long elapsed = System.currentTimeMillis() - start;
        System.out.format("Benchmark time: %.3f s\n", elapsed / 1.e3);
    }

    public List<Map<String, String>> run() throws Exception {
        long startTime = System.currentTimeMillis();
        DataFrame df = loadData();
        long elapsed = System.currentTimeMillis() - startTime;

        System.out.format("Loading time: %d ms\n", elapsed);
        System.out.format("%d rows\n", df.getNumRows());

        List<Map<String, String>> results = new ArrayList<>();

        APLExplanation trueOutput = warmStart(df);
        int numTrueResults = trueOutput.getResults().size();

        for (double sr : sampleRates) {
            for (double osf : outlierSampleFractions) {
                System.out.format("Sample rate %.4f, outlier sample fraction %.4f\n", sr, osf);
                for (int curTrial = 0; curTrial < numTrials; curTrial++) {
                    System.gc();

                    PercentileClassifier classifier = getClassifier(sr, osf);
                    startTime = System.currentTimeMillis();
                    classifier.process(df);
                    long classificationTime = System.currentTimeMillis() - startTime;
                    DataFrame classifiedDF = classifier.getResults();

                    APLSummarizer summarizer = getSummarizer(classifier.getOutputColumnName(), classifier, sr);

                    startTime = System.currentTimeMillis();
                    summarizer.process(classifiedDF);
                    long summarizationTime = System.currentTimeMillis() - startTime;
                    APLExplanation output = summarizer.getResults();
                    int numMatches = getNumMatches(output, trueOutput);

                    Map<String, String> curResults = new HashMap<>();
                    curResults.put("dataset", fileName);
                    curResults.put("trial", String.format("%d", curTrial));
                    curResults.put("sample_rate", String.format("%f", sr));
                    curResults.put("outlier_sample_fraction", String.format("%f", osf));
                    curResults.put("classification_time", String.format("%d", classificationTime));
                    curResults.put("summarization_time", String.format("%d", summarizationTime));
                    curResults.put("cutoff_time", String.format("%f", classifier.cutoffTime));
                    curResults.put("sampling_time", String.format("%f", classifier.classificationTime));
                    curResults.put("encoding_time", String.format("%f", summarizer.encodingTime));
                    curResults.put("explanation_time", String.format("%f", summarizer.explanationTime));

                    APriori kernel;
                    if (useBasic) {
                        kernel = summarizer.aplBasicKernel;
                    } else {
                        kernel = summarizer.aplKernel;
                    }
                    curResults.put("shard_time", String.format("%f", kernel.shardTime));
                    curResults.put("initialization_time", String.format("%f", kernel.initializationTime));
                    curResults.put("rowstore_time", String.format("%f", kernel.rowstoreTime));
                    curResults.put("order1_time", String.format("%f", kernel.explainTime[0]));
                    curResults.put("order2_time", String.format("%f", kernel.explainTime[1]));
                    curResults.put("order3_time", String.format("%f", kernel.explainTime[2]));
                    curResults.put("num_results_o1", String.format("%d", kernel.numSaved[0]));
                    curResults.put("num_results_o2", String.format("%d", kernel.numSaved[1]));
                    curResults.put("num_results_o3", String.format("%d", kernel.numSaved[2]));
                    curResults.put("num_next_o1", String.format("%d", kernel.numNext[0]));
                    curResults.put("num_next_o2", String.format("%d", kernel.numNext[1]));

                    curResults.put("num_results", String.format("%d", summarizer.numResults));
                    curResults.put("num_encoded", String.format("%d", summarizer.numEncodedCategories));
                    curResults.put("recall", String.format("%f", (double) numMatches / numTrueResults));
                    curResults.put("precision", String.format("%f", (double) numMatches / summarizer.numResults));
                    results.add(curResults);
                }
            }
        }

        return results;
    }

    public APLExplanation warmStart(DataFrame df) throws Exception {
        long start = System.currentTimeMillis();
        APLExplanation trueOutput = null;
        while (System.currentTimeMillis() - start < 1000 * warmupTime || trueOutput == null) {
            System.gc();

            PercentileClassifier classifier = getClassifier(1.0, -1.0);
            classifier.process(df);
            DataFrame classifiedDF = classifier.getResults();

            APLSummarizer summarizer = getSummarizer(classifier.getOutputColumnName(), classifier, 1.0);

            summarizer.process(classifiedDF);
            trueOutput = summarizer.getResults();
        }
        return trueOutput;
    }

    public int getNumMatches(APLExplanation output, APLExplanation trueOutput) {
        List<APLExplanationResult> results = output.getResults();
        List<APLExplanationResult> trueResults = trueOutput.getResults();

        int numResults = results.size();
        int trueNumResults = trueResults.size();
        int numMatches = 0;

        for (APLExplanationResult result : results) {
            for (APLExplanationResult trueResult : trueResults) {
                if (result.equals(trueResult, output.getEncoder(), trueOutput.getEncoder())) {
                    numMatches++;
                    break;
                }
            }
        }

        return numMatches;
    }

    public PercentileClassifier getClassifier(double sampleRate, double outlierSampleFraction) throws MacroBaseException {
        PercentileClassifier classifier = new PercentileClassifier(metric);
        classifier.setPercentile(cutoff);
        classifier.setIncludeHigh(pctileHigh);
        classifier.setIncludeLow(pctileLow);
        classifier.setSampleRate(sampleRate);
        classifier.setOutlierSampleFraction(outlierSampleFraction);
//        classifier.setOutlierSampleSize(outlierSampleSize);
//        classifier.setInlierSampleSize(inlierSampleSize);
        classifier.setVerbose(false);
        return classifier;
    }

    public APLSummarizer getSummarizer(String outlierColumnName, Classifier classifier, double sampleRate) throws MacroBaseException {
        APLOutlierSummarizer summarizer = new APLOutlierSummarizer();
        summarizer.setOutlierColumn(outlierColumnName);
        summarizer.setAttributes(attributes);
        summarizer.setMinSupport(minSupport);
        summarizer.setMinRatioMetric(minRiskRatio);
        summarizer.setRatioMetric(ratioMetric);
        summarizer.setNumThreads(Runtime.getRuntime().availableProcessors());
//                summarizer.setSampleRate(sampleRate);
        summarizer.setInlierWeight(classifier.getInlierWeight());
        summarizer.setOutlierSampleRate(classifier.getOutlierSampleRate());
        summarizer.setBasic(useBasic);
        summarizer.setSimpleEncoding(simpleEncoding);
        summarizer.setCalcErrors(sampleRate < 1.0);
        summarizer.setFullNumOutliers(classifier.getNumOutliers());
        summarizer.setVerbose(false);
        return summarizer;
    }

    public DataFrame loadData() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put(metric, Schema.ColType.DOUBLE);
        List<String> requiredColumns = new ArrayList<>(attributes);
        requiredColumns.add(metric);

        CSVDataFrameParser loader = new CSVDataFrameParser(fileName, requiredColumns);
        loader.setColumnTypes(colTypes);
        DataFrame df = loader.load();
        return df;
    }
}
