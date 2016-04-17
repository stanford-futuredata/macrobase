package macrobase.analysis.pipeline;

import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.pipeline.operator.MBOperatorChain;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class BasicBatchedPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(BasicBatchedPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        long startMs = System.currentTimeMillis();
        DataIngester ingester = conf.constructIngester();
       
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
                (result.getNumInliers() + result.getNumOutliers()) / (double) totalMs * 1000);

        return Arrays.asList(new AnalysisResult(result.getNumOutliers(),
                result.getNumInliers(),
                loadMs,
                executeMs,
                summarizeMs,
                result.getItemsets()));
    }
}