package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.classify.ArithmeticClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.CubeClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PredicateCubeClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.QuantileClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.RawClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLMeanSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLOutlierSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameWriter;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default pipeline for cubed data: load, classify, and then explain
 */
public class CubePipeline implements Pipeline {

    Logger log = LoggerFactory.getLogger("CubePipeline");

    // Ingest
    private String inputURI;
    private Map<String, String> restHeader;
    private Map<String, Object> jsonBody;
    private boolean usePost;

    // Classifiers
    private String classifierType;
    private String countColumn;
    private double cutoff;
    private String strCutoff;
    private boolean isStrPredicate;

    private String predicateStr;
    private Optional<String> metric;

    private boolean includeHi;
    private boolean includeLo;
    private Optional<String> meanColumn;
    private Optional<String> stdColumn;
    private LinkedHashMap<String, Double> quantileColumns;

    // Explanation
    private List<String> attributes;
    private double minSupport;
    private double minRatioMetric;

    private boolean debugDump;

    public CubePipeline(PipelineConfig conf) {
        inputURI = conf.get("inputURI");
        restHeader = conf.get("restHeader", null);
        jsonBody = conf.get("jsonBody", null);
        usePost = conf.get("usePost", true);

        classifierType = conf.get("classifier", "arithmetic");
        countColumn = conf.get("countColumn", "count");

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

        predicateStr = conf.get("predicate", "==").trim();
        metric = Optional.ofNullable(conf.get("metric"));

        includeHi = conf.get("includeHi", true);
        includeLo = conf.get("includeLo", true);
        meanColumn = Optional.ofNullable(conf.get("meanColumn"));
        stdColumn = Optional.ofNullable(conf.get("stdColumn"));
        quantileColumns = conf.get("quantileColumns", new LinkedHashMap<String, Double>());

        attributes = conf.get("attributes");
        minSupport = conf.get("minSupport", 3.0);
        minRatioMetric = conf.get("minRatioMetric", 0.01);

        debugDump = conf.get("debugDump", false);
    }

    public APLExplanation results() throws Exception {
        Map<String, Schema.ColType> colTypes = getColTypes();
        long startTime = System.currentTimeMillis();
        final List<String> requiredColumns = new ArrayList<>(attributes);
        requiredColumns.add(countColumn);
        metric.ifPresent(requiredColumns::add);
        meanColumn.ifPresent(requiredColumns::add);
        stdColumn.ifPresent(requiredColumns::add);
        requiredColumns.addAll(quantileColumns.keySet());
        DataFrame df = PipelineUtils.loadDataFrame(
            inputURI,
            colTypes,
            restHeader,
            jsonBody,
            usePost,
            requiredColumns
        );
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Loading time: {}", elapsed);
        log.info("{} rows", df.getNumRows());
        log.info("Attributes: {}", attributes);

        CubeClassifier classifier = getClassifier();
        startTime = System.currentTimeMillis();
        classifier.process(df);
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Classification time: {}", elapsed);
        df = classifier.getResults();
        if (debugDump) {
            CSVDataFrameWriter writer = new CSVDataFrameWriter();
            PrintWriter out = new PrintWriter("classified.csv");
            writer.writeToStream(df, out);
        }

        startTime = System.currentTimeMillis();
        APLSummarizer summarizer = getSummarizer(classifier);
        summarizer.process(df);
        APLExplanation explanation = summarizer.getResults();
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Summarization time: {}", elapsed);

        return explanation;
    }

    private Map<String, Schema.ColType> getColTypes() throws MacrobaseException {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put(countColumn, Schema.ColType.DOUBLE);
        switch (classifierType) {
            case "meanshift":
            case "arithmetic": {
                colTypes.put(meanColumn
                        .orElseThrow(() -> new MacrobaseException("mean column not present in config")),
                    Schema.ColType.DOUBLE);
                colTypes.put(stdColumn
                        .orElseThrow(() -> new MacrobaseException("std column not present in config")),
                    Schema.ColType.DOUBLE);
                return colTypes;
            }
            case "quantile": {
                for (String col : quantileColumns.keySet()) {
                    colTypes.put(col, Schema.ColType.DOUBLE);
                }
                return colTypes;
            }
            case "predicate": {
                if (isStrPredicate) {
                    colTypes.put(metric.orElseThrow(
                        () -> new MacrobaseException("metric column not present in config")),
                        Schema.ColType.STRING);
                } else {
                    colTypes.put(metric.orElseThrow(
                        () -> new MacrobaseException("metric column not present in config")),
                        Schema.ColType.DOUBLE);
                }
                return colTypes;
            }
            case "raw": {
                colTypes.put(meanColumn.orElseThrow(
                    () -> new MacrobaseException("mean column not present in config")),
                    Schema.ColType.DOUBLE);
            }
            default:
                throw new MacrobaseException("Bad Classifier Name");
        }
    }

    private CubeClassifier getClassifier() throws MacrobaseException {
        switch (classifierType) {
            case "arithmetic": {
                ArithmeticClassifier classifier =
                    new ArithmeticClassifier(countColumn, meanColumn.orElseThrow(
                        () -> new MacrobaseException("mean column not present in config")),
                        stdColumn.orElseThrow(
                            () -> new MacrobaseException("std column not present in config")));
                classifier.setPercentile(cutoff);
                classifier.setIncludeHigh(includeHi);
                classifier.setIncludeLow(includeLo);
                return classifier;
            }
            case "quantile": {
                QuantileClassifier classifier =
                    new QuantileClassifier(countColumn, quantileColumns);
                classifier.setPercentile(cutoff);
                classifier.setIncludeHigh(includeHi);
                classifier.setIncludeLow(includeLo);
                return classifier;
            }
            case "predicate": {
                if (isStrPredicate) {
                    return new PredicateCubeClassifier(countColumn,
                        metric.orElseThrow(
                            () -> new MacrobaseException("metric column not present in config")),
                        predicateStr, strCutoff);
                }
                return new PredicateCubeClassifier(countColumn,
                    metric.orElseThrow(
                        () -> new MacrobaseException("metric column not present in config")),
                    predicateStr, cutoff);
            }

            case "meanshift":
            case "raw": {
                return new RawClassifier(
                    countColumn,
                    meanColumn.orElseThrow(
                        () -> new MacrobaseException("mean column not present in config"))
                );
            }
            default:
                throw new MacrobaseException("Bad Classifier Name");
        }
    }

    private APLSummarizer getSummarizer(CubeClassifier classifier) throws Exception {
        switch (classifierType) {
            case "meanshift": {
                APLMeanSummarizer summarizer = new APLMeanSummarizer();
                summarizer.setCountColumn(countColumn);
                summarizer.setMeanColumn(meanColumn.orElseThrow(
                        () -> new MacrobaseException("mean column not present in config")));
                summarizer.setStdColumn(stdColumn.orElseThrow(
                        () -> new MacrobaseException("std column not present in config")));
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinStdDev(minRatioMetric);
                return summarizer;
            }
            default: {
                APLOutlierSummarizer summarizer = new APLOutlierSummarizer();
                summarizer.setOutlierColumn(classifier.getOutputColumnName());
                summarizer.setCountColumn(classifier.getCountColumnName());
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinRatioMetric(minRatioMetric);
                return summarizer;
            }
        }
    }
}
