package macrobase.analysis.pipeline;

import macrobase.analysis.classify.EWAppxPercentileOutlierClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.EWStreamingSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import macrobase.analysis.transform.EWFeatureTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class BasicOneShotEWStreamingPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(BasicOneShotEWStreamingPipeline.class);

    @Override
    public void initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
    }

    @Override
    public AnalysisResult run() throws Exception {
        long startMs = System.currentTimeMillis();
        DataIngester ingester = conf.constructIngester();
        List<Datum> data = ingester.getStream().drain();
        long loadEndMs = System.currentTimeMillis();

        FeatureTransform featureTransform = new EWFeatureTransform(conf);
        featureTransform.consume(ingester.getStream().drain());

        OutlierClassifier outlierClassifier = new EWAppxPercentileOutlierClassifier(conf);
        outlierClassifier.consume(featureTransform.getStream().drain());

        Summarizer summarizer = new EWStreamingSummarizer(conf);
        summarizer.consume(outlierClassifier.getStream().drain());

        Summary result = summarizer.getStream().drain().get(0);

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
