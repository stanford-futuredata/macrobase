package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.conf.Config;

import java.util.List;

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
