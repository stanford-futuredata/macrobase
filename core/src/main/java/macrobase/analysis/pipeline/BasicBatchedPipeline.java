package macrobase.analysis.pipeline;

import com.google.common.base.Stopwatch;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LowMetricTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BasicBatchedPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(BasicBatchedPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        DataIngester ingester = conf.constructIngester();
        List<Datum> data = ingester.getStream().drain();

        if(conf.isSet(MacroBaseConf.LOW_METRIC_TRANSFORM)) {
            LowMetricTransform lmt = new LowMetricTransform(conf);
            lmt.consume(data);
            data = lmt.getStream().drain();
        }

        System.gc();
        final long loadMs = sw.elapsed(TimeUnit.MILLISECONDS);

        FeatureTransform ft = new BatchScoreFeatureTransform(conf);
        ft.consume(data);

        OutlierClassifier oc = new BatchingPercentileClassifier(conf);

        oc.consume(ft.getStream().drain());

        Summarizer bs = new BatchSummarizer(conf);
        bs.consume(oc.getStream().drain());
        Summary result = bs.summarize().getStream().drain().get(0);

        final long totalMs = sw.elapsed(TimeUnit.MILLISECONDS) - loadMs;
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