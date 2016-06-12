package macrobase.bench.experimental;

import com.google.common.base.Stopwatch;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RuleFusionPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(RuleFusionPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    private class RuleBasedClassifier extends OutlierClassifier {
        MBStream<OutlierClassificationResult> out = new MBStream<>();
        @Override
        public void initialize() throws Exception {

        }

        @Override
        public void consume(List<Datum> records) throws Exception {
            List<OutlierClassificationResult> results = new ArrayList<>();
            for(Datum d : records) {
                if(d.getMetrics().getEntry(d.getMetrics().getDimension()-1) < .45) {
                    results.add(new OutlierClassificationResult(d, true));
                } else {
                    results.add(new OutlierClassificationResult(d, false));
                }
            }

            out.add(results);
        }

        @Override
        public void shutdown() throws Exception {

        }

        @Override
        public MBStream<OutlierClassificationResult> getStream() throws Exception {
            return out;
        }
    }

    private class LogicalAndEnsembler extends OutlierClassifier {
        MBStream<OutlierClassificationResult> out = new MBStream<>();
        public LogicalAndEnsembler(List<OutlierClassificationResult> left,
                                   List<OutlierClassificationResult> right) {
            List<OutlierClassificationResult> results = new ArrayList<>(left.size());
            assert(left.size() == right.size());
            for(int i = 0; i < left.size(); ++i) {
                if(left.get(i).isOutlier()) {
                    results.add(left.get(i));
                } else if(right.get(i).isOutlier()) {
                    results.add(right.get(i));
                } else {
                    results.add(left.get(i));
                }
            }

            out.add(results);
        }

        @Override
        public void initialize() throws Exception {

        }

        @Override
        public void consume(List<Datum> records) throws Exception {
            assert (records == null);

        }

        @Override
        public void shutdown() throws Exception {

        }

        @Override
        public MBStream<OutlierClassificationResult> getStream() throws Exception {
            return out;
        }
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        DataIngester ingester = conf.constructIngester();
        List<Datum> data = ingester.getStream().drain();
        System.gc();
        final long loadMs = sw.elapsed(TimeUnit.MILLISECONDS);

        FeatureTransform ft = new BatchScoreFeatureTransform(conf, conf.getTransformType());
        ft.consume(data);

        OutlierClassifier unsupervised_oc = new BatchingPercentileClassifier(conf);
        unsupervised_oc.consume(ft.getStream().drain());

        RuleBasedClassifier rbc = new RuleBasedClassifier();
        rbc.consume(data);

        LogicalAndEnsembler lae = new LogicalAndEnsembler(rbc.getStream().drain(),
                                                          unsupervised_oc.getStream().drain());

        Summarizer bs = new BatchSummarizer(conf);
        bs.consume(lae.getStream().drain());
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