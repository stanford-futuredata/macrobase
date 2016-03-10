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
import macrobase.ingest.DataIngester;
import macrobase.ingest.TimedBatchIngest;
import macrobase.analysis.transform.EWFeatureTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class BasicOneShotEWStreamingPipeline extends OneShotPipeline {
    private static final Logger log = LoggerFactory.getLogger(BasicOneShotEWStreamingPipeline.class);

    public BasicOneShotEWStreamingPipeline(MacroBaseConf conf) throws ConfigurationException, SQLException, IOException {
        super(conf);
        conf.sanityCheckStreaming();
    }

    @Override
    AnalysisResult run() throws SQLException, IOException, ConfigurationException {
        long startMs = System.currentTimeMillis();
        DataIngester ingester = conf.constructIngester();
        TimedBatchIngest batchIngest = new TimedBatchIngest(ingester);
        FeatureTransform featureTransform = new EWFeatureTransform(conf, batchIngest);
        OutlierClassifier outlierClassifier = new EWAppxPercentileOutlierClassifier(conf, featureTransform);
        Summarizer summarizer = new EWStreamingSummarizer(conf, outlierClassifier);
        Summary result = summarizer.next();

        final long endMs = System.currentTimeMillis();
        final long loadMs = batchIngest.getFinishTimeMs() - startMs;
        final long totalMs = endMs - batchIngest.getFinishTimeMs();
        final long summarizeMs = result.getCreationTimeMs();
        final long executeMs = totalMs - summarizeMs;

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
