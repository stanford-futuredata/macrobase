package macrobase.analysis.pipeline;

import com.google.common.base.Stopwatch;
import macrobase.MacroBase;
import macrobase.analysis.classify.EWAppxPercentileOutlierClassifier;
import macrobase.analysis.pipeline.operator.MBOperator;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.summary.EWStreamingSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import macrobase.analysis.transform.EWFeatureTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BasicOneShotEWStreamingPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(BasicOneShotEWStreamingPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        final int batchSize = conf.getInt(MacroBaseConf.TUPLE_BATCH_SIZE,
                                          MacroBaseDefaults.TUPLE_BATCH_SIZE);

        Stopwatch sw = Stopwatch.createStarted();
        DataIngester ingester = conf.constructIngester();
        List<Datum> data = ingester.getStream().drain();
        System.gc();
        final long loadMs = sw.elapsed(TimeUnit.MILLISECONDS);

        MBStream<Datum> streamData = new MBStream<>(data);

        MBOperator<Datum, OutlierClassificationResult> ocr =
                new EWFeatureTransform(conf)
                .then(new EWAppxPercentileOutlierClassifier(conf), batchSize);

        while(streamData.remaining() > 0) {
            ocr.consume(streamData.drain(batchSize));
        }

        MBStream<OutlierClassificationResult> ocrs = ocr.getStream();

        Summarizer summarizer = new EWStreamingSummarizer(conf);
        summarizer.consume(ocrs.drain());

        Summary result = summarizer.getStream().drain().get(0);

        final long totalMs = sw.elapsed(TimeUnit.MILLISECONDS) - loadMs;
        final long summarizeMs = result.getCreationTimeMs();
        final long executeMs = totalMs - result.getCreationTimeMs();

        log.info("took {}ms ({} tuples/sec)",
                 totalMs,
                 (result.getNumInliers()+result.getNumOutliers())/(double)totalMs*1000);

        return Arrays.asList(new AnalysisResult(result.getNumOutliers(),
                                  result.getNumInliers(),
                                  loadMs,
                                  executeMs,
                                  summarizeMs,
                                  result.getItemsets()));
    }
}
