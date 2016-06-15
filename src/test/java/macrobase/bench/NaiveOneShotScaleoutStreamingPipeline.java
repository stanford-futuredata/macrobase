package macrobase.bench;

import com.google.common.base.Stopwatch;
import macrobase.analysis.classify.EWAppxPercentileOutlierClassifier;
import macrobase.analysis.contextualoutlier.Interval;
import macrobase.analysis.pipeline.BasePipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.pipeline.operator.MBOperator;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.EWStreamingSummarizer;
import macrobase.analysis.summary.Summarizer;
import macrobase.analysis.summary.Summary;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.transform.EWFeatureTransform;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.LinearMetricNormalizer;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class NaiveOneShotScaleoutStreamingPipeline extends BasePipeline {
    private static final Logger log = LoggerFactory.getLogger(NaiveOneShotScaleoutStreamingPipeline.class);

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        super.initialize(conf);
        conf.sanityCheckBatch();
        return this;
    }

    public static final String SCALEOUT = "macrobase.naive.scaleout.factor";
    public static final int NUM_PASSES = 5;

    @Override
    public List<AnalysisResult> run() throws Exception {
        final int batchSize = conf.getInt(MacroBaseConf.TUPLE_BATCH_SIZE,
                                          MacroBaseDefaults.TUPLE_BATCH_SIZE);

        Stopwatch sw = Stopwatch.createStarted();
        DataIngester ingester = conf.constructIngester();
        FeatureTransform normalizer = new LinearMetricNormalizer();
        normalizer.consume(ingester.getStream().drain());
        List<Datum> data = normalizer.getStream().drain();
        System.gc();
        final long loadMs = sw.elapsed(TimeUnit.MILLISECONDS);

        final Integer parallelism = conf.getInt(SCALEOUT);

        List<List<Datum>> partitionedData = new ArrayList<>();

        for(int i = 0; i < parallelism; ++i) {
            partitionedData.add(new ArrayList<>());
        }

        for(int i = 0; i < (data.size()/parallelism)*parallelism; ++i) {
            partitionedData.get(i % parallelism).add(data.get(i));
        }

        ExecutorService executor = Executors.newFixedThreadPool(parallelism);

        Semaphore startSem = new Semaphore(0);
        Semaphore readySem = new Semaphore(0);


        Vector<Summary> summaries = new Vector<>();

        for(List<Datum> partition : partitionedData) {
            executor.submit(() -> {
                    try {

                        MBStream<Datum> streamData = new MBStream<>();

                        for(int i = 0; i < NUM_PASSES; ++i) {
                            streamData.add(partition);
                        }

                        readySem.release();
                        startSem.acquireUninterruptibly();

                        Summarizer summarizer = new EWStreamingSummarizer(conf);
                        MBOperator<Datum, Summary> pipeline =
                                new EWFeatureTransform(conf)
                                        .then(new EWAppxPercentileOutlierClassifier(conf), batchSize)
                                        .then(summarizer, batchSize);

                        while (streamData.remaining() > 0) {
                            pipeline.consume(streamData.drain(batchSize));
                        }

                        Summary result = summarizer.summarize().getStream().drain().get(0);
                        summaries.add(result);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        }

        long st = System.currentTimeMillis();
        startSem.release(parallelism);
        executor.shutdown();
        executor.awaitTermination(1000, TimeUnit.DAYS);
        long totalMs = System.currentTimeMillis() - st;

        final long summarizeMs = totalMs;
        final long executeMs = totalMs;

        log.info("took {}ms ({} tuples/sec)",
                 totalMs,
                 (data.size()) / (double) totalMs * 1000);

        List<ItemsetResult> isr = new ArrayList<>();

        for (Summary s : summaries) {
            isr.addAll(s.getItemsets());
        }

        return Arrays.asList(new AnalysisResult(0,
                                  0,
                                  loadMs,
                                  executeMs,
                                  summarizeMs,
                                  isr));
    }
}
