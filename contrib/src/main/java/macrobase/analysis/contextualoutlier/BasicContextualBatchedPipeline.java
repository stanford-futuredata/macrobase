package macrobase.analysis.contextualoutlier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import macrobase.analysis.contextualoutlier.conf.ContextualConf;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.OutlierClassificationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.ContextualAnalysisResult;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summary;
import macrobase.conf.MacroBaseConf;
import macrobase.analysis.contextualoutlier.conf.ContextualConf.ContextualAPI;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;

public class BasicContextualBatchedPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(BasicContextualBatchedPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        List<AnalysisResult> allARs = new ArrayList<AnalysisResult>();
        long time1 = System.currentTimeMillis();
        ContextualAPI contextualAPI = ContextualConf.getContextualAPI(conf);

        //load the data
        DataIngester ingester = conf.constructIngester();
        List<Datum> data = ingester.getStream().drain();

        ContextualTransformer transformer = new ContextualTransformer(conf);
        transformer.consume(data);
        List<ContextualDatum> cdata = transformer.getStream().drain();

        ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(conf);
        Map<Context, List<OutlierClassificationResult>> context2Outliers = null;
        long time2 = System.currentTimeMillis();
        //invoke different contextual outlier detection APIs
        if (contextualAPI == ContextualAPI.findAllContextualOutliers) {
            context2Outliers = contextualDetector.searchContextualOutliers(cdata);
        } else if (contextualAPI == ContextualAPI.findContextsGivenOutlierPredicate) {
            context2Outliers = contextualDetector.searchContextGivenOutliers(cdata);
        }
        long time3 = System.currentTimeMillis();
        long loadMs = time2 - time1;
        long executeMs = time3 - time2;
        //summarize every contextual outliers found
        for (Context context : context2Outliers.keySet()) {
            log.info("Context: " + context.print(conf.getEncoder()));
            BatchSummarizer summarizer = new BatchSummarizer(conf);
            summarizer.consume(context2Outliers.get(context));
            Summary result = summarizer.summarize().getStream().drain().get(0);
            long summarizeMs = result.getCreationTimeMs();
            ContextualAnalysisResult ar = new ContextualAnalysisResult(context, result.getNumOutliers(),
                                                                       result.getNumInliers(),
                                                                       loadMs,
                                                                       executeMs,
                                                                       summarizeMs,
                                                                       result.getItemsets());
            allARs.add(ar);
        }
        return allARs;
    }
}
