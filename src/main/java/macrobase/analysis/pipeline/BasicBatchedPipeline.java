package macrobase.analysis.pipeline;

import com.google.common.collect.Lists;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.contextualoutlier.Context;
import macrobase.analysis.contextualoutlier.ContextualOutlierDetector;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.stats.BatchTrainScore;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import macrobase.ingest.TimedBatchIngest;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BasicBatchedPipeline extends OneShotPipeline {
    private static final Logger log = LoggerFactory.getLogger(BasicBatchedPipeline.class);

    public BasicBatchedPipeline(MacroBaseConf conf) throws ConfigurationException, SQLException, IOException {
        super(conf);
        conf.sanityCheckBatch();
    }

    public AnalysisResult contextualAnalyze(DataIngester ingester) throws SQLException, IOException, ConfigurationException{
        BatchTrainScore detector = conf.constructTransform(conf.getTransformType());

    	List<Datum> data = Lists.newArrayList(ingester);
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
    AnalysisResult run() throws SQLException, IOException, ConfigurationException {
        long startMs = System.currentTimeMillis();
        DataIngester ingester = conf.constructIngester();
        TimedBatchIngest batchIngest = new TimedBatchIngest(ingester);
        FeatureTransform featureTransform = new BatchScoreFeatureTransform(conf, batchIngest, conf.getTransformType());
        OutlierClassifier outlierClassifier = new BatchingPercentileClassifier(conf, featureTransform);
        BatchSummarizer summarizer = new BatchSummarizer(conf, outlierClassifier);

        // TODO: this should be a new pipeline
    	if(contextualEnabled){
    		return contextualAnalyze(ingester);
    	}

        Summary result = summarizer.next();

        final long endMs = System.currentTimeMillis();
        final long loadMs = batchIngest.getFinishTimeMs() - startMs;
        final long totalMs = endMs - batchIngest.getFinishTimeMs();
        final long summarizeMs = result.getCreationTimeMs();
        final long executeMs = totalMs - result.getCreationTimeMs();

        log.info("took {}ms ({} tuples/sec)",
                 totalMs,
                 (result.getNumInliers()+result.getNumOutliers())/(double)totalMs*1000);

        return new AnalysisResult(result.getNumInliers(),
                                  result.getNumOutliers(),
                                  loadMs,
                                  executeMs,
                                  summarizeMs,
                                  result.getItemsets());
    }
}
