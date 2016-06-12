package macrobase.bench.experimental;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.pipeline.operator.MBGroupBy;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.stats.FFT;
import macrobase.analysis.stats.Truncate;
import macrobase.analysis.summary.BatchSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.transform.BatchScoreFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import macrobase.analysis.classify.BatchingPercentileClassifier;
import macrobase.analysis.classify.DumpClassifier;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.result.AnalysisResult;
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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GroupedFFTPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(GroupedFFTPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    private class SFFTFeatureTransform extends FeatureTransform {

        MBStream<Datum> output = new MBStream<>();
        @Override
        public void initialize() throws Exception {

        }

        @Override
        public void consume(List<Datum> records) throws Exception {
            int windowSize = conf.getInt(MacroBaseConf.WINDOW_SIZE);
            int k = conf.getInt(MacroBaseConf.TRUNCATE_K);

            FFT fft = new FFT(conf);
            Truncate truncate = new Truncate(conf);

            List<Datum> windows = new ArrayList<>(records.size()/windowSize);
            for(int i = 0; i < records.size(); i += windowSize) {
                ArrayRealVector rv = new ArrayRealVector(windowSize);
                for(int j = 0; j < windowSize; ++j) {
                    rv.setEntry(j, records.get(i+j).getMetrics().getNorm());
                }
                windows.add(new Datum(records.get(i).attributes(), rv));
            }

            fft.consume(windows);

            truncate.consume(fft.getStream().drain());
            output.add(truncate.getStream().drain());
        }

        @Override
        public void shutdown() throws Exception {

        }

        @Override
        public MBStream<Datum> getStream() throws Exception {
            return output;
        }
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        DataIngester ingester = conf.constructIngester();
        List<Datum> data = ingester.getStream().drain();
        System.gc();
        final long loadMs = sw.elapsed(TimeUnit.MILLISECONDS);

        MBGroupBy gb = new MBGroupBy(Lists.newArrayList(0),
                                     () -> new SFFTFeatureTransform());

        Stopwatch fttimer = Stopwatch.createStarted();
        gb.consume(data);

        List<Datum> gbout = gb.getStream().drain();
        log.info("GBOUT SIZE {}", gbout.size());

        fttimer.stop();
        Stopwatch resttimer = Stopwatch.createStarted();
        FeatureTransform ft = new BatchScoreFeatureTransform(conf, conf.getTransformType());
        ft.consume(gbout);

        OutlierClassifier oc = new BatchingPercentileClassifier(conf);

        oc.consume(ft.getStream().drain());

        Summarizer bs = new BatchSummarizer(conf);
        bs.consume(oc.getStream().drain());
        Summary result = bs.summarize().getStream().drain().get(0);

        resttimer.stop();
        final long totalMs = sw.elapsed(TimeUnit.MILLISECONDS) - loadMs;
        final long summarizeMs = result.getCreationTimeMs();
        final long executeMs = totalMs - result.getCreationTimeMs();

        log.info("ft took {}, rest took {}", fttimer.elapsed(TimeUnit.MILLISECONDS),
                 resttimer.elapsed(TimeUnit.MILLISECONDS));

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