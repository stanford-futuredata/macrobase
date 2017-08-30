package edu.stanford.futuredata.macrobase.contrib.aria;

import edu.stanford.futuredata.macrobase.analysis.classify.ArithmeticClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.CubeClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.QuantileClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.apriori.APrioriSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameWriter;
import edu.stanford.futuredata.macrobase.pipeline.Pipeline;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Aria pipeline for cubed data: load, classify, and then explain
 */
public class AriaCubePipeline implements Pipeline {
    Logger log = LoggerFactory.getLogger("AriaCubePipeline");

    // API ingester params
    private String inputURI;
    private String startTimeStamp;
    private String endTimeStamp;
    private Map<String, String> restHeader;
    private boolean prunedLoading;

    private String metric;
    private String classifierType;
    private double cutoff;
    private boolean includeHi;
    private boolean includeLo;

    private List<String> attributes;
    private double minSupport;
    private double minRatioMetric;

    public AriaCubePipeline(PipelineConfig conf) {
        inputURI = conf.get("inputURI");
        startTimeStamp = conf.get("startTime");
        endTimeStamp = conf.get("endTime");
        restHeader = conf.get("restHeader");
        prunedLoading = conf.get("prunedLoading", true);

        metric = conf.get("metric");
        classifierType = conf.get("classifier");
        cutoff = conf.get("cutoff", 1.0);
        includeHi = conf.get("includeHi", true);
        includeLo = conf.get("includeLo", false);

        attributes = conf.get("attributes");
        minSupport = conf.get("minSupport", 0.01);
        minRatioMetric = conf.get("minRatioMetric", 3.0);
    }

    public Explanation results() throws Exception {
        long startTime = System.currentTimeMillis();
        CubeQueryService cs = new CubeQueryService(
                inputURI,
                restHeader,
                startTimeStamp,
                endTimeStamp
        );
        DataFrame df;
        if (prunedLoading) {
            df = cs.getFrequentCubeEntries(
                    metric,
                    attributes,
                    getOperations(),
                    minSupport
            );
        } else {
            df = cs.getAllCubeEntries(
                    metric,
                    attributes,
                    getOperations()
            );
        }
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Loading time: {}", elapsed);
        log.info("{} rows", df.getNumRows());
        log.info("Attributes: {}", attributes);

        String outlierColumnName = "Sum";
        String countColumName = "Count";
        startTime = System.currentTimeMillis();
        CubeClassifier classifier = getClassifier();
        if (classifier != null) {
            classifier.process(df);
            elapsed = System.currentTimeMillis() - startTime;
            log.info("Classification time: {}", elapsed);
            log.info("Outlier cutoffs: {} {}",
                    classifier.getLowCutoff(),
                    classifier.getHighCutoff()
            );
            df = classifier.getResults();
            outlierColumnName = classifier.getOutputColumnName();
            countColumName = classifier.getCountColumnName();
        }
        CSVDataFrameWriter writer = new CSVDataFrameWriter();
        PrintWriter out = new PrintWriter("df.csv");
        writer.writeToStream(df, out);

        APrioriSummarizer summarizer = new APrioriSummarizer();
        summarizer.setOutlierColumn(outlierColumnName);
        summarizer.setCountColumn(countColumName);
        summarizer.setAttributes(attributes);
        summarizer.setMinSupport(minSupport);
        summarizer.setMinRatioMetric(minRatioMetric);
        startTime = System.currentTimeMillis();
        summarizer.process(df);
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Summarization time: {}", elapsed);
        Explanation output = summarizer.getResults();
        return output;
    }

    public List<String> getOperations() throws MacrobaseException {
        switch (classifierType) {
            case "quantile": {
                return CubeQueryService.quantileOperators;
            }
            case "raw": {
                return Arrays.asList("Count", "Sum");
            }
            default:
                throw new MacrobaseException("Unsupported classifier");
        }
    }


    public CubeClassifier getClassifier() throws MacrobaseException {
        switch (classifierType) {
            case "arithmetic": {
                ArithmeticClassifier classifier =
                        new ArithmeticClassifier(
                                "Count",
                                "Average",
                                "StandardDeviation"
                        );
                classifier.setPercentile(cutoff);
                classifier.setIncludeHigh(includeHi);
                classifier.setIncludeLow(includeLo);
                return classifier;
            }
            case "quantile": {
                LinkedHashMap<String, Double> percentileColumns = new LinkedHashMap<>();
                percentileColumns.put("Percentile001", 0.001);
                percentileColumns.put("Percentile01", 0.01);
                percentileColumns.put("Percentile05", 0.05);
                percentileColumns.put("Percentile50", 0.50);
                percentileColumns.put("Percentile95", 0.95);
                percentileColumns.put("Percentile99", 0.99);
                percentileColumns.put("Percentile999", 0.999);
                QuantileClassifier classifier =
                        new QuantileClassifier("Count", percentileColumns);
                classifier.setPercentile(cutoff);
                classifier.setIncludeHigh(includeHi);
                classifier.setIncludeLow(includeLo);
                return classifier;
            }
            case "raw": {
                return null;
            }
            default:
                throw new MacrobaseException("Bad Classifier Name");
        }
    }
}
