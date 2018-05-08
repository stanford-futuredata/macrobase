import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.*;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import io.CSVOutput;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BasicSupportBench {
    private String testName;
    private String fileName;

    private List<String> attributes;
    private double minSupport;

    private List<Double> sampleRates;
    private int numTrials;
    private int warmupTime;
    private boolean smartStopping;
    private boolean doContainment;
    private boolean computeAccuracy;
    private int maxOrder;

    private boolean verbose = false;
    private boolean calcError = false;
    private boolean appendTimeStamp = true;

    public BasicSupportBench(String confFile) throws IOException{
        RunConfig conf = RunConfig.fromJsonFile(confFile);
        testName = conf.get("testName");
        fileName = conf.get("fileName");

        attributes = conf.get("attributes");
        minSupport = conf.get("minSupport", 0.01);

        sampleRates = conf.get("sampleRates");
        numTrials = conf.get("numTrials");
        warmupTime = conf.get("warmupTime", 5);
        smartStopping = conf.get("smartStopping", true);
        doContainment = conf.get("doContainment", true);
        computeAccuracy = conf.get("computeAccuracy", true);
        maxOrder = conf.get("maxOrder", 3);

        verbose = conf.get("verbose", false);
        calcError = conf.get("calcError", false);
        appendTimeStamp = conf.get("appendTimeStamp", false);
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        String confFile = args[0];
        BasicSupportBench bench = new BasicSupportBench(confFile);

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
            System.out.format("Sample rate %.4f\n", sr);
            for (int curTrial = 0; curTrial < numTrials; curTrial++) {
                System.gc();

                DataFrame dfCopy = df;
                long start = System.nanoTime();
                if (sr < 1.0) {
                    dfCopy = dfCopy.sample(sr);
                }
                double sampleTime = (System.nanoTime() - start) / 1.e6;

                APLSummarizer summarizer = getSummarizer(dfCopy, sr);

                startTime = System.currentTimeMillis();
                summarizer.process(dfCopy);
                long summarizationTime = System.currentTimeMillis() - startTime;
                int numMatches = 0;
                if (computeAccuracy) {
                    APLExplanation output = summarizer.getResults();
                    numMatches = getNumMatches(output, trueOutput);
                }

                Map<String, String> curResults = new HashMap<>();
                curResults.put("dataset", fileName);
                curResults.put("trial", String.format("%d", curTrial));
                curResults.put("sample_rate", String.format("%f", sr));
                curResults.put("sampling_time", String.format("%f", sampleTime));
                curResults.put("summarization_time", String.format("%d", summarizationTime));
                curResults.put("encoding_time", String.format("%f", summarizer.encodingTime));
                curResults.put("explanation_time", String.format("%f", summarizer.explanationTime));
                curResults.put("initialization_time", String.format("%f", summarizer.aplBasicKernel.initializationTime));
                curResults.put("rowstore_time", String.format("%f", summarizer.aplBasicKernel.rowstoreTime));
                curResults.put("order1_time", String.format("%f", summarizer.aplBasicKernel.explainTime[0]));
                curResults.put("order2_time", String.format("%f", summarizer.aplBasicKernel.explainTime[1]));
                curResults.put("order3_time", String.format("%f", summarizer.aplBasicKernel.explainTime[2]));
                curResults.put("order1_agg_time", String.format("%f", summarizer.aplBasicKernel.aggregationTime[0]));
                curResults.put("order2_agg_time", String.format("%f", summarizer.aplBasicKernel.aggregationTime[1]));
                curResults.put("order3_agg_time", String.format("%f", summarizer.aplBasicKernel.aggregationTime[2]));
                curResults.put("order1_prune_time", String.format("%f", summarizer.aplBasicKernel.pruneTime[0]));
                curResults.put("order2_prune_time", String.format("%f", summarizer.aplBasicKernel.pruneTime[1]));
                curResults.put("order3_prune_time", String.format("%f", summarizer.aplBasicKernel.pruneTime[2]));
                curResults.put("order1_save_time", String.format("%f", summarizer.aplBasicKernel.saveTime[0]));
                curResults.put("order2_save_time", String.format("%f", summarizer.aplBasicKernel.saveTime[1]));
                curResults.put("order3_save_time", String.format("%f", summarizer.aplBasicKernel.saveTime[2]));
                curResults.put("num_results", String.format("%d", summarizer.numResults));
                curResults.put("num_results_o1", String.format("%d", summarizer.aplBasicKernel.numSaved[0]));
                curResults.put("num_results_o2", String.format("%d", summarizer.aplBasicKernel.numSaved[1]));
                curResults.put("num_results_o3", String.format("%d", summarizer.aplBasicKernel.numSaved[2]));
                curResults.put("num_next_o1", String.format("%d", summarizer.aplBasicKernel.numNext[0]));
                curResults.put("num_next_o2", String.format("%d", summarizer.aplBasicKernel.numNext[1]));
                curResults.put("num_encoded", String.format("%d", summarizer.numEncodedCategories));
                if (computeAccuracy) {
                    curResults.put("recall", String.format("%f", (double) numMatches / numTrueResults));
                    curResults.put("precision", String.format("%f", (double) numMatches / summarizer.numResults));
                }
                results.add(curResults);
            }
        }

        return results;
    }

    public APLExplanation warmStart(DataFrame df) throws Exception {
        long start = System.currentTimeMillis();
        APLExplanation trueOutput = null;
        while (System.currentTimeMillis() - start < 1000 * warmupTime || trueOutput == null) {
            System.gc();

            APLSummarizer summarizer = getSummarizer(df, 1.0);
            summarizer.process(df);
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

    public APLSummarizer getSummarizer(DataFrame input, double sampleRate) throws MacroBaseException {
        APLSupportSummarizer summarizer = new APLSupportSummarizer();
        summarizer.setAttributes(attributes);
        summarizer.setMinSupport(minSupport);
        summarizer.setNumThreads(Runtime.getRuntime().availableProcessors());
//                summarizer.setSampleRate(sampleRate);
        summarizer.setCalcErrors(sampleRate < 1.0);
        summarizer.setFullNumOutliers(input.getNumRows());
        summarizer.setVerbose(false);
        summarizer.setBasic(true);
        summarizer.setSmartStopping(smartStopping);
        summarizer.setDoContainment(doContainment);
        summarizer.setMaxOrder(maxOrder);
        return summarizer;
    }

    public DataFrame loadData() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        List<String> requiredColumns = new ArrayList<>(attributes);

        CSVDataFrameParser loader = new CSVDataFrameParser(fileName, requiredColumns);
        loader.setColumnTypes(colTypes);
        DataFrame df = loader.load();
        return df;
    }
}
