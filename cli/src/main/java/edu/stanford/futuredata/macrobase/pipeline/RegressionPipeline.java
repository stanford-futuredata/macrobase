package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.classify.ArithmeticClassifier;
import edu.stanford.futuredata.macrobase.analysis.classify.CubeClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.APrioriSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.RegressionSummarizer;
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
public class RegressionPipeline implements Pipeline {
    Logger log = LoggerFactory.getLogger("RegressionPipeline");
    private String inputFile;

    private boolean includeHi;
    private boolean includeLo;
    private String countColumn;
    private String meanColumn;
    private String maxColumn;
    private String stdColumn;

    private List<String> attributes;
    private int minCount;
    private double minStd;


    public RegressionPipeline(Config conf) {
        inputFile = conf.getAs("inputFile");

        includeHi = conf.getAs("includeHi");
        includeLo = conf.getAs("includeLo");
        countColumn = conf.getAs("countColumn");
        meanColumn = conf.getAs("meanColumn");
        maxColumn = conf.getAs("maxColumn");
        stdColumn = conf.getAs("stdColumn");

        attributes = conf.getAs("attributes");
        minCount = conf.getAs("minCount");
        minStd = conf.getAs("minStd");
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

        RegressionSummarizer summarizer = new RegressionSummarizer();
        summarizer.setCountColumn(countColumn);
        summarizer.setMeanColumn(meanColumn);
        summarizer.setMaxColumn(maxColumn);
        summarizer.setAttributes(attributes);
        summarizer.setMinCount(minCount);
        summarizer.setMinStd(minStd);
        startTime = System.currentTimeMillis();
        summarizer.process(df);
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Summarization time: {}", elapsed);
        Explanation output = summarizer.getResults();

        System.out.println(output.prettyPrint());
    }

    private Map<String, Schema.ColType> getColTypes() {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put(countColumn, Schema.ColType.DOUBLE);
        colTypes.put(meanColumn, Schema.ColType.DOUBLE);
        colTypes.put(stdColumn, Schema.ColType.DOUBLE);
        colTypes.put(maxColumn, Schema.ColType.DOUBLE);
        return colTypes;
    }
}
