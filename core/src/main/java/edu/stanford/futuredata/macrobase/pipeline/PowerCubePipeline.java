package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.*;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Pipeline for cubed data that has min, max and moment aggregates
 */
public class PowerCubePipeline implements Pipeline {
    Logger log = LoggerFactory.getLogger("PowerCubePipeline");

    // Ingest
    private String inputURI;
    private Map<String, String> restHeader;
    private Map<String, Object> jsonBody;
    private boolean usePost;

    // Classifiers
    private double cutoff;
//    private boolean includeHi;
//    private boolean includeLo;
    private int ka;
    private int kb;
    private Optional<String> minColumn;
    private Optional<String> maxColumn;
    private Optional<String> logMinColumn;
    private Optional<String> logMaxColumn;
    private List<String> powerSumColumns;
    private List<String> logSumColumns;

    // Explanation
    private List<String> attributes;
    private double minSupport;
    private double minRatioMetric;

    public PowerCubePipeline(PipelineConfig conf) {
        inputURI = conf.get("inputURI");
        restHeader = conf.get("restHeader", null);
        jsonBody = conf.get("jsonBody", null);
        usePost = conf.get("usePost", true);

        cutoff = conf.get("cutoff", 1.0);

        minColumn = Optional.ofNullable(conf.get("minColumn"));
        maxColumn = Optional.ofNullable(conf.get("maxColumn"));
        powerSumColumns = conf.get("powerSumColumns", new ArrayList<String>());
        ka = powerSumColumns.size();
        logMinColumn = Optional.ofNullable(conf.get("logMinColumn"));
        logMaxColumn = Optional.ofNullable(conf.get("logMaxColumn"));
        logSumColumns = conf.get("logSumColumns", new ArrayList<String>());
        kb = logSumColumns.size();

        attributes = conf.get("attributes");
        minSupport = conf.get("minSupport", 3.0);
        minRatioMetric = conf.get("minRatioMetric", 0.01);
    }

    public APLExplanation results() throws Exception {
        Map<String, Schema.ColType> colTypes = getColTypes();
        long startTime = System.currentTimeMillis();
        final List<String> requiredColumns = new ArrayList<>(attributes);
        requiredColumns.addAll(colTypes.keySet());
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

        startTime = System.currentTimeMillis();
        APLSummarizer summarizer = getSummarizer();
        summarizer.process(df);
        APLExplanation explanation = summarizer.getResults();
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Summarization time: {}", elapsed);

        return explanation;
    }

    private Map<String, Schema.ColType> getColTypes() throws MacroBaseException {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        if (ka > 0) {
            colTypes.put(minColumn
                            .orElseThrow(() -> new MacroBaseException("min column not present in config")),
                    Schema.ColType.DOUBLE);
            colTypes.put(maxColumn
                            .orElseThrow(() -> new MacroBaseException("max column not present in config")),
                    Schema.ColType.DOUBLE);
            for (String col : powerSumColumns) {
                colTypes.put(col, Schema.ColType.DOUBLE);
            }
        }
        if (kb > 0) {
            colTypes.put(logMinColumn
                            .orElseThrow(() -> new MacroBaseException("log min column not present in config")),
                    Schema.ColType.DOUBLE);
            colTypes.put(logMaxColumn
                            .orElseThrow(() -> new MacroBaseException("log max column not present in config")),
                    Schema.ColType.DOUBLE);
            for (String col : logSumColumns) {
                colTypes.put(col, Schema.ColType.DOUBLE);
            }
        }
        return colTypes;
    }

    private APLSummarizer getSummarizer() throws Exception {
        APLMomentSummarizer summarizer = new APLMomentSummarizer();
        summarizer.setKa(ka);
        if (ka > 0) {
            summarizer.setMinColumn(minColumn.orElseThrow(
                    () -> new MacroBaseException("min column not present in config")));
            summarizer.setMaxColumn(maxColumn.orElseThrow(
                    () -> new MacroBaseException("max column not present in config")));
            summarizer.setPowerSumColumns(powerSumColumns);
        }
        summarizer.setKb(kb);
        if (kb > 0) {
            summarizer.setLogMinColumn(logMinColumn.orElseThrow(
                    () -> new MacroBaseException("log min column not present in config")));
            summarizer.setLogMaxColumn(logMaxColumn.orElseThrow(
                    () -> new MacroBaseException("log max column not present in config")));
            summarizer.setLogSumColumns(logSumColumns);
        }

        summarizer.setAttributes(attributes);
        summarizer.setMinSupport(minSupport);
        summarizer.setMinRatioMetric(minRatioMetric);
        summarizer.setQuantileCutoff(1.0 - cutoff/100.0);
        return summarizer;
    }
}
