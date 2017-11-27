package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.classify.ArithmeticClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.CubeClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.QuantileClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.RawClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.apriori.APExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.apriori.APrioriSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameWriter;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    private String classifierType;
    private String countColumn;
    private String meanColumn;
    private String stdColumn;
    private LinkedHashMap<String, Double> quantileColumns;
    private double cutoff;
    private boolean includeHi;
    private boolean includeLo;

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
        meanColumn = conf.get("meanColumn", "mean");
        stdColumn = conf.get("stdColumn", "std");
        quantileColumns = conf.get("quantileColumns", new LinkedHashMap<String, Double>());
        cutoff = conf.get("cutoff", 1.0);
        includeHi = conf.get("includeHi", true);
        includeLo = conf.get("includeLo", true);

        attributes = conf.get("attributes");
        minSupport = conf.get("minSupport", 3.0);
        minRatioMetric = conf.get("minRatioMetric", 0.01);

        debugDump = conf.get("debugDump", false);
    }

    public Explanation results() throws Exception {
        Map<String, Schema.ColType> colTypes = getColTypes();
        long startTime = System.currentTimeMillis();
        DataFrame df = PipelineUtils.loadDataFrame(
                inputURI,
                colTypes,
                restHeader,
                jsonBody,
                usePost
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
        log.info("Outlier cutoffs: {} {}",
                classifier.getLowCutoff(),
                classifier.getHighCutoff()
        );
        df = classifier.getResults();
        if (debugDump) {
            CSVDataFrameWriter writer = new CSVDataFrameWriter();
            PrintWriter out = new PrintWriter("classified.csv");
            writer.writeToStream(df, out);
        }

        APrioriSummarizer summarizer = new APrioriSummarizer();
        summarizer.setOutlierColumn(classifier.getOutputColumnName());
        summarizer.setCountColumn(classifier.getCountColumnName());
        summarizer.setAttributes(attributes);
        summarizer.setMinSupport(minSupport);
        summarizer.setMinRatioMetric(minRatioMetric);
        startTime = System.currentTimeMillis();
        summarizer.process(df);
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Summarization time: {}", elapsed);
        APExplanation output = summarizer.getResults();
        return output;
    }

    private Map<String, Schema.ColType> getColTypes() throws MacrobaseException {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        switch (classifierType) {
            case "arithmetic": {
                colTypes.put(countColumn, Schema.ColType.DOUBLE);
                colTypes.put(meanColumn, Schema.ColType.DOUBLE);
                colTypes.put(stdColumn, Schema.ColType.DOUBLE);
                return colTypes;
            }
            case "quantile": {
                colTypes.put(countColumn, Schema.ColType.DOUBLE);
                for (String col : quantileColumns.keySet()) {
                    colTypes.put(col, Schema.ColType.DOUBLE);
                }
                return colTypes;
            }
            case "raw": {
                colTypes.put(countColumn, Schema.ColType.DOUBLE);
                colTypes.put(meanColumn, Schema.ColType.DOUBLE);
            }
            default:
                throw new MacrobaseException("Bad Classifier Name");
        }
    }

    private CubeClassifier getClassifier() throws MacrobaseException {
        switch (classifierType) {
            case "arithmetic": {
                ArithmeticClassifier classifier =
                        new ArithmeticClassifier(countColumn, meanColumn, stdColumn);
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
            case "raw": {
                return new RawClassifier(
                        countColumn,
                        meanColumn
                );
            }
            default:
                throw new MacrobaseException("Bad Classifier Name");
        }
    }
}
