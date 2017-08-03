package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.classify.ArithmeticClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.CubeClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.apriori.APrioriSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default pipeline for cubed data: load, classify, and then explain
 */
public class CubePipeline implements Pipeline {
    Logger log = LoggerFactory.getLogger("CubePipeline");

    private String inputURI;

    private String classifierType;
    private String countColumn;
    private String meanColumn;
    private String stdColumn;
    private double percentile;
    private boolean includeHi;
    private boolean includeLo;

    private List<String> attributes;
    private double minSupport;
    private double minRiskRatio;

    public CubePipeline(PipelineConfig conf) {
        inputURI = conf.get("inputURI");

        classifierType = conf.get("classifier");
        countColumn = conf.get("countColumn");
        meanColumn = conf.get("meanColumn");
        stdColumn = conf.get("stdColumn");
        percentile = conf.get("percentile");
        includeHi = conf.get("includeHi");
        includeLo = conf.get("includeLo");

        attributes = conf.get("attributes");
        minSupport = conf.get("minSupport");
        minRiskRatio = conf.get("minRiskRatio");
    }

    public Explanation results() throws Exception {
        Map<String, Schema.ColType> colTypes = getColTypes();
        long startTime = System.currentTimeMillis();
        DataFrame df = PipelineUtils.loadDataFrame(inputURI, colTypes);
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

        APrioriSummarizer summarizer = new APrioriSummarizer();
        summarizer.setOutlierColumn(classifier.getOutputColumnName());
        summarizer.setCountColumn(classifier.getCountColumnName());
        summarizer.setAttributes(attributes);
        summarizer.setMinSupport(minSupport);
        summarizer.setMinRiskRatio(minRiskRatio);
        startTime = System.currentTimeMillis();
        summarizer.process(df);
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Summarization time: {}", elapsed);
        Explanation output = summarizer.getResults();
        return output;
    }

    private Map<String, Schema.ColType> getColTypes() {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        switch (classifierType) {
            case "arithmetic": {
                colTypes.put(countColumn, Schema.ColType.DOUBLE);
                colTypes.put(meanColumn, Schema.ColType.DOUBLE);
                colTypes.put(stdColumn, Schema.ColType.DOUBLE);
            }
        }
        return colTypes;
    }

    private CubeClassifier getClassifier() throws MacrobaseException {
        switch (classifierType) {
            case "arithmetic": {
                ArithmeticClassifier classifier =
                        new ArithmeticClassifier(countColumn, meanColumn, stdColumn);
                classifier.setPercentile(percentile);
                classifier.setIncludeHigh(includeHi);
                classifier.setIncludeLow(includeLo);
                return classifier;
            }
            default:
                throw new MacrobaseException("Bad Classifier Name");
        }
    }
}
