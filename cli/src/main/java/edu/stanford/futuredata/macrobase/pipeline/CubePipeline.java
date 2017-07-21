package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.classify.ArithmeticClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.CubeClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.APrioriSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.FPGrowthSummarizer;
import edu.stanford.futuredata.macrobase.conf.Config;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
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
    private String inputFile;

    private String classifierType;

    private double percentile;
    private boolean includeHi;
    private boolean includeLo;
    private String count;
    private String mean;
    private String std;

    private List<String> attributes;
    private double minSupport;
    private double minRiskRatio;


    public CubePipeline(Config conf) {
        inputFile = conf.getAs("inputFile");

        classifierType = conf.getAs("classifier");

        percentile = conf.getAs("percentile");
        includeHi = conf.getAs("includeHi");
        includeLo = conf.getAs("includeLo");
        count = conf.getAs("count");
        mean = conf.getAs("mean");
        std = conf.getAs("std");

        attributes = conf.getAs("attributes");
        minSupport = conf.getAs("minSupport");
        minRiskRatio = conf.getAs("minRiskRatio");
    }

    @Override
    public void run() throws Exception {
        Map<String, Schema.ColType> colTypes = getColTypes();
        CSVDataFrameLoader loader = new CSVDataFrameLoader(inputFile);
        loader.setColumnTypes(colTypes);
        long startTime = System.currentTimeMillis();
        DataFrame df = loader.load();
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Loading time: {}", elapsed);
        log.info("{} rows", df.getNumRows());
        log.info("Attributes: {}", attributes);

        CubeClassifier classifier = getClassifier();
        classifier.process(df);
        df = classifier.getResults();
        log.info("Outlier cutoffs: {} {}",
                classifier.getLowCutoff(),
                classifier.getHighCutoff()
        );

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

        System.out.println(output.prettyPrint());
    }

    private Map<String, Schema.ColType> getColTypes() {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        switch (classifierType) {
            case "arithmetic": {
                colTypes.put(count, Schema.ColType.DOUBLE);
                colTypes.put(mean, Schema.ColType.DOUBLE);
                colTypes.put(std, Schema.ColType.DOUBLE);
            }
        }
        return colTypes;
    }

    private CubeClassifier getClassifier() {
        switch (classifierType) {
            case "arithmetic": {
                ArithmeticClassifier classifier =
                        new ArithmeticClassifier(count, mean, std);
                classifier.setPercentile(percentile);
                classifier.setIncludeHigh(includeHi);
                classifier.setIncludeLow(includeLo);
                return classifier;
            }
            default:
                return null;
        }
    }
}
