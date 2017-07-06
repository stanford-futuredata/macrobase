package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.groupby.GroupBySummarizer;
import edu.stanford.futuredata.macrobase.conf.Config;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVEncoderPipeline implements Pipeline {
    private String inputFile;

    private String metric;
    private double percentile;
    private boolean includeHi;
    private boolean includeLo;

    private List<String> attributes;

    private String outputFile;

    public CSVEncoderPipeline(Config conf) {
        inputFile = conf.getAs("inputFile");

        metric = conf.getAs("metric");
        percentile = conf.getAs("percentile");
        includeHi = conf.getAs("includeHi");
        includeLo = conf.getAs("includeLo");

        attributes = conf.getAs("attributes");

        outputFile = conf.getAs("outputFile");
    }

    public void load() throws Exception {

    }

    @Override
    public void run() throws Exception {

    }
}
