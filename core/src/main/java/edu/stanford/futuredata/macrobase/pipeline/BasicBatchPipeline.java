package edu.stanford.futuredata.macrobase.pipeline;

import com.fasterxml.jackson.databind.jsonschema.SchemaAware;
import edu.stanford.futuredata.macrobase.analysis.classify.*;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLCountMeanShiftSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLOutlierSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLXRaySummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGrowthSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Simplest default pipeline: load, classify, and then explain
 * Only supports operating over a single metric
 */
public class BasicBatchPipeline implements Pipeline {
    Logger log = LoggerFactory.getLogger(Pipeline.class);

    private String inputURI = null;

    private String classifierType;
    private String metric;
    private double cutoff;
    private Optional<String> meanColumn;
    private String strCutoff;
    private boolean isStrPredicate;
    private boolean pctileHigh;
    private boolean pctileLow;
    private String predicateStr;
    private int numThreads;
    private int bitmapRatioThreshold;

    private String summarizerType;
    private List<String> attributes;
    private String ratioMetric;
    private double minSupport;
    private double minRiskRatio;
    private double meanShiftRatio;

    private boolean useFDs;
    private int[] functionalDependencies;


    public BasicBatchPipeline (PipelineConfig conf) {
        inputURI = conf.get("inputURI");

        classifierType = conf.get("classifier", "percentile");
        metric = conf.get("metric");

        if (classifierType.equals("predicate") || classifierType.equals("countmeanshift")){
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
        bitmapRatioThreshold = conf.get("bitmapRatioThreshold", 256);


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
        meanColumn = Optional.ofNullable(conf.get("meanColumn"));
        meanShiftRatio = conf.get("meanShiftRatio", 1.0);
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
            case "countmeanshift": {
                if (isStrPredicate) {
                    return new CountMeanShiftClassifier(
                            metric,
                            meanColumn.orElseThrow(
                                    () -> new MacroBaseException("mean column not present in config")), predicateStr,
                            strCutoff);
                } else {
                    return new CountMeanShiftClassifier(
                            metric,
                            meanColumn.orElseThrow(
                                    () -> new MacroBaseException("mean column not present in config")), predicateStr,
                            cutoff);
                }
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

    public BatchSummarizer getSummarizer(String outlierColumnName) throws MacroBaseException {
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
                summarizer.setBitmapRatioThreshold(bitmapRatioThreshold);
                summarizer.setNumThreads(numThreads);
                summarizer.setFDUsage(useFDs);
                summarizer.setFDValues(functionalDependencies);
                return summarizer;
            }
            case "xray": {
                APLXRaySummarizer summarizer = new APLXRaySummarizer(true);
                summarizer.setOutlierColumn(outlierColumnName);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinRatioMetric(minRiskRatio);
                summarizer.setBitmapRatioThreshold(bitmapRatioThreshold);
                summarizer.setNumThreads(numThreads);
                summarizer.setFDUsage(useFDs);
                summarizer.setFDValues(functionalDependencies);
                return summarizer;
            }
            case "countmeanshift": {
                APLCountMeanShiftSummarizer summarizer = new APLCountMeanShiftSummarizer();
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinMeanShift(meanShiftRatio);
                summarizer.setNumThreads(numThreads);
                return summarizer;
            }
            default: {
                throw new MacroBaseException("Bad Summarizer Type");
            }
        }
    }

    public DataFrame loadData() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        if (isStrPredicate) {
            colTypes.put(metric, Schema.ColType.STRING);
        }
        else{
            colTypes.put(metric, Schema.ColType.DOUBLE);
        }
        List<String> requiredColumns = new ArrayList<>(attributes);
        if (meanColumn.isPresent()) {
            colTypes.put(meanColumn.get(), Schema.ColType.DOUBLE);
            requiredColumns.add(meanColumn.get());

        }
        requiredColumns.add(metric);
        return PipelineUtils.loadDataFrame(inputURI, colTypes, requiredColumns);
    }

    @Override
    public Explanation results() throws Exception {
        long startTime = System.currentTimeMillis();
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
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Summarization time: {} ms", elapsed);
        Explanation output = summarizer.getResults();

        return output;
    }
}
