package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.ItemsetBatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.groupby.GroupBySummarizer;
import edu.stanford.futuredata.macrobase.conf.Config;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;

import java.security.acl.Group;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupByPipeline implements Pipeline {
    private String inputFile;

    private String metric;
    private double percentile;
    private boolean includeHi;
    private boolean includeLo;

    private List<String> attributes;
    private double minSupport;
    private double minRiskRatio;


    public GroupByPipeline(Config conf) {
        inputFile = conf.getAs("inputFile");

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
        DataFrame df = loader.load();
        System.out.println(df.getNumRows());
        System.out.println(attributes);

        PercentileClassifier classifier = new PercentileClassifier(metric);
        classifier.setPercentile(percentile);
        classifier.setIncludeHigh(includeHi);
        classifier.setIncludeLow(includeLo);
        classifier.process(df);
        df = classifier.getResults();
        System.out.println("Outlier Cutoff is: "+classifier.getHighCutoff());

        GroupBySummarizer summarizer = new GroupBySummarizer();
        summarizer.setOutlierColumn(classifier.getOutputColumnName());
        summarizer.setAttributes(attributes);
        summarizer.setMinSupport(minSupport);
        summarizer.setMinRiskRatio(minRiskRatio);
        summarizer.process(df);
    }
}
