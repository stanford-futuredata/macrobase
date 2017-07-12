package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
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

import java.util.*;

public class BatchPipeline implements Pipeline {
    Logger log = LoggerFactory.getLogger("BatchPipeline");
    private String inputFile;

    private String summarizerType;

    private String metric;
    private double percentile;
    private boolean includeHi;
    private boolean includeLo;

    private List<String> attributes;
    private double minSupport;
    private double minRiskRatio;


    public BatchPipeline(Config conf) {
        inputFile = conf.getAs("inputFile");

        summarizerType = conf.getAs("summarizer");

        metric = conf.getAs("metric");
        percentile = conf.getAs("percentile");
        includeHi = conf.getAs("includeHi");
        includeLo = conf.getAs("includeLo");

        attributes = conf.getAs("attributes");
        minSupport = conf.getAs("minSupport");
        minRiskRatio = conf.getAs("minRiskRatio");
    }

    @Override
    public void run() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put(metric, Schema.ColType.DOUBLE);
        CSVDataFrameLoader loader = new CSVDataFrameLoader(inputFile);
        loader.setColumnTypes(colTypes);
        long startTime = System.currentTimeMillis();
        DataFrame df = loader.load();
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Loading time: {}", elapsed);
        log.info("{} rows", df.getNumRows());
        log.info("Attributes: {}", attributes);

        PercentileClassifier classifier = new PercentileClassifier(metric);
        classifier.setPercentile(percentile);
        classifier.setIncludeHigh(includeHi);
        classifier.setIncludeLow(includeLo);
        classifier.process(df);
        df = classifier.getResults();
        log.info("Outlier cutoffs: {} {}",
                classifier.getLowCutoff(),
                classifier.getHighCutoff()
        );

        BatchSummarizer summarizer = getSummarizer(classifier.getOutputColumnName());
        startTime = System.currentTimeMillis();
        summarizer.process(df);
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Summarization time: {}", elapsed);
        Explanation output = summarizer.getResults();

        System.out.println(output.prettyPrint());
    }

    private BatchSummarizer getSummarizer(String outlierColumn) {
        switch (summarizerType) {
            case "fpgrowth": {
                FPGrowthSummarizer summarizer = new FPGrowthSummarizer();
                summarizer.setOutlierColumn(outlierColumn);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setUseAttributeCombinations(true);
                summarizer.setMinRiskRatio(minRiskRatio);
                return summarizer;
            }
            case "apriori": {
                APrioriSummarizer summarizer = new APrioriSummarizer();
                summarizer.setOutlierColumn(outlierColumn);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinRiskRatio(minRiskRatio);
                return summarizer;
            }
            default:
                return null;
        }
    }
}
