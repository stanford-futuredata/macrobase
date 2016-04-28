package macrobase.analysis.pipeline;

import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.contextualoutlier.Context;
import macrobase.analysis.contextualoutlier.ContextualOutlierDetector;
import macrobase.analysis.pipeline.operator.MBOperatorChain;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summary;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BasicBatchedPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(BasicBatchedPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    public AnalysisResult contextualAnalyze() throws Exception {
        BatchTrainScore detector = conf.constructTransform(conf.getTransformType());

        DataIngester ingester = conf.constructIngester();

        List<Datum> data = ingester.getStream().drain();
    	ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(detector,
    			contextualDiscreteAttributes,contextualDoubleAttributes,contextualDenseContextTau,contextualNumIntervals);
    	
    	contextualDetector.searchContextualOutliers(data, targetPercentile);
    	Map<Context,BatchTrainScore.BatchResult> context2Outliers = contextualDetector.getContextualOutliers();
        for(Context context: context2Outliers.keySet()){
        	log.info("Context: " + context.print(conf.getEncoder()));
        	log.info("Number of Inliers: " + context2Outliers.get(context).getInliers().size());
        	log.info("Number of Outliers: " + context2Outliers.get(context).getOutliers().size());
      
        }
    	//explain the contextual outliers

        return new AnalysisResult(0, 0, 0, 0, 0, new ArrayList<>());
    }

    @Override
    public AnalysisResult run() throws Exception {
        // TODO: this should be a new pipeline
        if(contextualEnabled){
            return contextualAnalyze();
        }

        long startMs = System.currentTimeMillis();
        DataIngester ingester = conf.constructIngester();
        // TODO: this should be a new pipeline
        // Needs to happen early before ingester is possibly consumed. Downstream stages are allowed to drain
        // upstream stages in construction.
        if(contextualEnabled){
            return contextualAnalyze();
        }

        List<Datum> data = ingester.getStream().drain();
        long loadEndMs = System.currentTimeMillis();

        OutlierClassifier outlierClassifier = new BatchingPercentileClassifier(conf);

        if (conf.getBoolean(MacroBaseConf.CLASSIFIER_DUMP)) {
            String queryName = conf.getString(MacroBaseConf.QUERY_NAME);
            outlierClassifier = new DumpClassifier(conf, outlierClassifier, queryName);
        }

        MBOperatorChain<?, Summary> chain = MBOperatorChain.begin(data)
                                          .then(new BatchScoreFeatureTransform(conf, conf.getTransformType()))
                                          .then(outlierClassifier)
                                          .then(new BatchSummarizer(conf));

        Summary result = chain.execute().drain().get(0);

        final long endMs = System.currentTimeMillis();
        final long loadMs = loadEndMs - startMs;
        final long totalMs = endMs - loadEndMs;
        final long summarizeMs = result.getCreationTimeMs();
        final long executeMs = totalMs - result.getCreationTimeMs();

        log.info("took {}ms ({} tuples/sec)",
                 totalMs,
                 (result.getNumInliers()+result.getNumOutliers())/(double)totalMs*1000);

        return new AnalysisResult(result.getNumOutliers(),
                                  result.getNumInliers(),
                                  loadMs,
                                  executeMs,
                                  summarizeMs,
                                  result.getItemsets());
    }
}
