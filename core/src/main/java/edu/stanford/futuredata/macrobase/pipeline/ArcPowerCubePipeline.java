package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLArcMomentSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Pipeline for cubed data that has min, max and moment aggregates
 */
public class ArcPowerCubePipeline implements Pipeline {
    Logger log = LoggerFactory.getLogger("ArcPowerCubePipeline");

    // Ingest
    private String inputURI;
    private Map<String, String> restHeader;
    private Map<String, Object> jsonBody;
    private boolean usePost;

    // Classifiers
    private double cutoff;
//    private boolean includeHi;
//    private boolean includeLo;
    private int k;
    private Optional<String> minColumn;
    private Optional<String> maxColumn;
    private List<String> powerSumColumns;

    // Explanation
    private List<String> attributes;
    private double minSupport;
    private double minRatioMetric;

    public ArcPowerCubePipeline(PipelineConfig conf) {
        inputURI = conf.get("inputURI");
        restHeader = conf.get("restHeader", null);
        jsonBody = conf.get("jsonBody", null);
        usePost = conf.get("usePost", true);

        cutoff = conf.get("cutoff", 1.0);

        minColumn = Optional.ofNullable(conf.get("minColumn"));
        maxColumn = Optional.ofNullable(conf.get("maxColumn"));
        powerSumColumns = conf.get("powerSumColumns", new ArrayList<String>());
        k = powerSumColumns.size();

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
//        CSVDataFrameWriter writer = new CSVDataFrameWriter();
//        PrintWriter out = new PrintWriter("df_power.csv");
//        writer.writeToStream(df, out);

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
        colTypes.put(
                minColumn.orElseThrow(() -> new MacroBaseException("min column not present in config")),
                Schema.ColType.DOUBLE);
        colTypes.put(
                maxColumn.orElseThrow(() -> new MacroBaseException("max column not present in config")),
                Schema.ColType.DOUBLE);
        for (String col : powerSumColumns) {
            colTypes.put(col, Schema.ColType.DOUBLE);
        }
        return colTypes;
    }

    private APLSummarizer getSummarizer() throws Exception {
        APLArcMomentSummarizer summarizer = new APLArcMomentSummarizer();
        summarizer.setK(k);
        summarizer.setMinColumn(minColumn.orElseThrow(
                () -> new MacroBaseException("min column not present in config")));
        summarizer.setMaxColumn(maxColumn.orElseThrow(
                () -> new MacroBaseException("max column not present in config")));
        summarizer.setPowerSumColumns(powerSumColumns);

        summarizer.setAttributes(attributes);
        summarizer.setMinSupport(minSupport);
        summarizer.setMinRatioMetric(minRatioMetric);
        summarizer.setQuantileCutoff(1.0 - cutoff/100.0);
        return summarizer;
    }
}
