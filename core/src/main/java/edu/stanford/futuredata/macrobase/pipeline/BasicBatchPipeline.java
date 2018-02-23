package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PredicateClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLOutlierSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.distributed.analysis.classify.DistributedClassifier;
import edu.stanford.futuredata.macrobase.distributed.analysis.classify.PredicateClassifierDistributed;
import edu.stanford.futuredata.macrobase.distributed.analysis.summary.DistributedBatchSummarizer;
import edu.stanford.futuredata.macrobase.distributed.analysis.summary.aplinearDistributed.APLOutlierSummarizerDistributed;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGrowthSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import edu.stanford.futuredata.macrobase.distributed.datamodel.DistributedDataFrame;
import edu.stanford.futuredata.macrobase.distributed.ingest.CSVDataFrameParserDistributed;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.*;

/**
 * Simplest default pipeline: load, classify, and then explain
 * Only supports operating over a single metric
 */
public class BasicBatchPipeline implements Pipeline {
    private Logger log = LogManager.getLogger(Pipeline.class);

    private String inputURI = null;

    private String classifierType;
    private String metric;
    private double cutoff;
    private String strCutoff;
    private boolean isStrPredicate;
    private boolean pctileHigh;
    private boolean pctileLow;
    private String predicateStr;
    private int numThreads;

    private String summarizerType;
    private List<String> attributes;
    private String ratioMetric;
    private double minSupport;
    private double minRiskRatio;

    private boolean useFDs;
    private int[] functionalDependencies;
    private String distributedMaster;
    private int distributedNumPartitions;

    private JavaSparkContext sparkContext;


    public BasicBatchPipeline (PipelineConfig conf) {
        inputURI = conf.get("inputURI");

        classifierType = conf.get("classifier", "percentile");
        metric = conf.get("metric");

        if (classifierType.equals("predicate")) {
            Object rawCutoff = conf.get("cutoff");
            isStrPredicate = rawCutoff instanceof String;
            if (isStrPredicate) {
                strCutoff = (String) rawCutoff;
            } else {
                cutoff = (double) rawCutoff;
            }
        } else {
            isStrPredicate = false;
            cutoff = conf.get("cutoff", 1.0);
        }

        pctileHigh = conf.get("includeHi",true);
        pctileLow = conf.get("includeLo", true);
        predicateStr = conf.get("predicate", "==").trim();

        summarizerType = conf.get("summarizer", "apriori");
        attributes = conf.get("attributes");
        ratioMetric = conf.get("ratioMetric", "globalRatio");
        minRiskRatio = conf.get("minRatioMetric", 3.0);
        minSupport = conf.get("minSupport", 0.01);
        numThreads = conf.get("numThreads", Runtime.getRuntime().availableProcessors());

        //if FDs are behind used, parse them into bitmaps. For now, all FDs must be in the first 31 attributes
        useFDs = conf.get("useFDs", false);
        if (useFDs) {
            ArrayList<ArrayList<Integer>> rawDependencies = conf.get("functionalDependencies");
            functionalDependencies = new int[attributes.size()];
            for (ArrayList<Integer> dependency : rawDependencies) {
                for (int i : dependency) {
                    for (int j : dependency) {
                        if (i != j) functionalDependencies[i] |= (1 << j);
                    }
                }
            }
        }
        distributedMaster = conf.get("distributedMaster", "local");
        distributedNumPartitions = conf.get("distributedNumPartitions", 1);
    }

    public Classifier getClassifier() throws MacroBaseException {
        switch (classifierType.toLowerCase()) {
            case "percentile": {
                PercentileClassifier classifier = new PercentileClassifier(metric);
                classifier.setPercentile(cutoff);
                classifier.setIncludeHigh(pctileHigh);
                classifier.setIncludeLow(pctileLow);
                return classifier;
            }
            case "predicate": {
                if (isStrPredicate){
                    PredicateClassifier classifier = new PredicateClassifier(metric, predicateStr, strCutoff);
                    return classifier;
                }
                PredicateClassifier classifier = new PredicateClassifier(metric, predicateStr, cutoff);
                return classifier;
            }
            default : {
                throw new MacroBaseException("Bad Classifier Type");
            }
        }
    }

<<<<<<< d92c91618cbb2c4186681691bb997ae31e271752
    public BatchSummarizer getSummarizer(String outlierColumnName) throws MacroBaseException {
=======
    private DistributedClassifier getDistributedClassifier() throws MacrobaseException {
        switch (classifierType.toLowerCase()) {
            case "predicate": {
                if (isStrPredicate){
                    PredicateClassifierDistributed classifier = new PredicateClassifierDistributed(metric, predicateStr, strCutoff);
                    return classifier;
                }
                PredicateClassifierDistributed classifier = new PredicateClassifierDistributed(metric, predicateStr, cutoff);
                return classifier;
            }
            default : {
                throw new MacrobaseException("Bad Classifier Type");
            }
        }
    }

    public ExplanationMetric getRatioMetric() throws MacrobaseException {
        switch (ratioMetric.toLowerCase()) {
            case "globalratio": {
                return new GlobalRatioMetric();
            }
            case "riskratio": {
                return new RiskRatioMetric();
            }
            default: {
                throw new MacrobaseException("Bad Ratio Metric");
            }
        }
    }

    private BatchSummarizer getSummarizer(String outlierColumnName) throws MacrobaseException {
>>>>>>> Fully working distributed pipeline
        switch (summarizerType.toLowerCase()) {
            case "fpgrowth": {
                FPGrowthSummarizer summarizer = new FPGrowthSummarizer();
                summarizer.setOutlierColumn(outlierColumnName);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinRiskRatio(minRiskRatio);
                summarizer.setUseAttributeCombinations(true);
                return summarizer;
            }
            case "aplinear":
            case "apriori": {
                APLOutlierSummarizer summarizer = new APLOutlierSummarizer(true);
                summarizer.setOutlierColumn(outlierColumnName);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinRatioMetric(minRiskRatio);
                summarizer.setNumThreads(numThreads);
                summarizer.setFDUsage(useFDs);
                summarizer.setFDValues(functionalDependencies);
                return summarizer;
            }
            default: {
                throw new MacrobaseException("Bad Summarizer Type");
            }
        }
    }

    private DistributedBatchSummarizer getDistributedSummarizer(String outlierColumnName) throws MacrobaseException {
        switch (summarizerType.toLowerCase()) {
            case "aplineardistributed": {
                APLOutlierSummarizerDistributed summarizer = new APLOutlierSummarizerDistributed();
                summarizer.setOutlierColumn(outlierColumnName);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinRatioMetric(minRiskRatio);
                summarizer.setNumPartitions(distributedNumPartitions);
                return summarizer;
            }
            default: {
                throw new MacroBaseException("Bad Summarizer Type");
            }
        }
    }

    private DataFrame loadData() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        if (isStrPredicate) {
            colTypes.put(metric, Schema.ColType.STRING);
        }
        else{
            colTypes.put(metric, Schema.ColType.DOUBLE);
        }
        List<String> requiredColumns = new ArrayList<>(attributes);
        requiredColumns.add(metric);
        return PipelineUtils.loadDataFrame(inputURI, colTypes, requiredColumns);
    }

    private DistributedDataFrame loadDataDistributed() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        if (isStrPredicate) {
            colTypes.put(metric, Schema.ColType.STRING);
        }
        else{
            colTypes.put(metric, Schema.ColType.DOUBLE);
        }
        List<String> requiredColumns = new ArrayList<>(attributes);
        requiredColumns.add(metric);
        CSVDataFrameParserDistributed loader = new CSVDataFrameParserDistributed(inputURI.substring(6), requiredColumns);
        loader.setColumnTypes(colTypes);
        DistributedDataFrame df = loader.load(sparkContext, distributedNumPartitions);
        return df;
    }

    @Override
    public Explanation results() throws Exception {
        long startTime = System.currentTimeMillis();
        Explanation output;
        if (!summarizerType.toLowerCase().equals("aplineardistributed")) {
            DataFrame df = loadData();
            long elapsed = System.currentTimeMillis() - startTime;

            log.info("Loading time: {} ms", elapsed);
            log.info("{} rows", df.getNumRows());
            log.info("Metric: {}", metric);
            log.info("Attributes: {}", attributes);

            Classifier classifier = getClassifier();
            classifier.process(df);
            df = classifier.getResults();

            BatchSummarizer summarizer = getSummarizer(classifier.getOutputColumnName());

            startTime = System.currentTimeMillis();
            summarizer.process(df);
            output = summarizer.getResults();
        } else {
            SparkConf conf = new SparkConf().setAppName("MacroBase");
            sparkContext = new JavaSparkContext(conf);
            DistributedDataFrame df = loadDataDistributed();
            long elapsed = System.currentTimeMillis() - startTime;

            log.info("Loading time: {} ms", elapsed);
            log.info("Metric: {}", metric);
            log.info("Attributes: {}", attributes);

            DistributedClassifier classifier = getDistributedClassifier();
            df = classifier.process(df);

            DistributedBatchSummarizer summarizer = getDistributedSummarizer(classifier.getOutputColumnName());

            startTime = System.currentTimeMillis();
            summarizer.process(df);
            output = summarizer.getResults();
        }
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Summarization time: {} ms", elapsed);

        return output;
    }
}
