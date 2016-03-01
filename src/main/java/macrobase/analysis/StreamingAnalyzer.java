package macrobase.analysis;

import com.google.common.base.Stopwatch;

import com.google.common.collect.Lists;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.sample.ExponentiallyBiasedAChao;
import macrobase.analysis.periodic.AbstractPeriodicUpdater;
import macrobase.analysis.periodic.TupleBasedRetrainer;
import macrobase.analysis.periodic.TupleAnalysisDecayer;
import macrobase.analysis.periodic.WallClockRetrainer;
import macrobase.analysis.periodic.WallClockAnalysisDecayer;
import macrobase.analysis.summary.itemset.ExponentiallyDecayingEmergingItemsets;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataLoader;
import macrobase.ingest.DatumEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class StreamingAnalyzer extends BaseAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(StreamingAnalyzer.class);

    //These are streaming-specific configuration parameters.
    //Non-streaming-specific parameters are set in BaseAnalyzer.
    private final Integer warmupCount;
    private final Integer inputReservoirSize;
    private final Integer scoreReservoirSize;
    private final Integer summaryPeriod;
    private final Boolean useRealTimePeriod;
    private final Boolean useTupleCountPeriod;
    private final Double decayRate;
    private final Integer modelRefreshPeriod;
    private final Integer outlierItemSummarySize;
    private final Integer inlierItemSummarySize;

    private final Semaphore startSemaphore;
    private final Semaphore endSemaphore;

    public StreamingAnalyzer(MacroBaseConf conf) throws ConfigurationException {
        super(conf);
        conf.sanityCheckStreaming();

        warmupCount = conf.getInt(MacroBaseConf.WARMUP_COUNT, MacroBaseDefaults.WARMUP_COUNT);
        inputReservoirSize = conf.getInt(MacroBaseConf.INPUT_RESERVOIR_SIZE, MacroBaseDefaults.INPUT_RESERVOIR_SIZE);
        scoreReservoirSize = conf.getInt(MacroBaseConf.SCORE_RESERVOIR_SIZE, MacroBaseDefaults.SCORE_RESERVOIR_SIZE);
        summaryPeriod = conf.getInt(MacroBaseConf.SUMMARY_UPDATE_PERIOD, MacroBaseDefaults.SUMMARY_UPDATE_PERIOD);
        useRealTimePeriod = conf.getBoolean(MacroBaseConf.USE_REAL_TIME_PERIOD,
                                            MacroBaseDefaults.USE_REAL_TIME_PERIOD);
        useTupleCountPeriod = conf.getBoolean(MacroBaseConf.USE_TUPLE_COUNT_PERIOD,
                                              MacroBaseDefaults.USE_TUPLE_COUNT_PERIOD);
        decayRate = conf.getDouble(MacroBaseConf.DECAY_RATE, MacroBaseDefaults.DECAY_RATE);
        modelRefreshPeriod = conf.getInt(MacroBaseConf.MODEL_UPDATE_PERIOD, MacroBaseDefaults.MODEL_UPDATE_PERIOD);
        outlierItemSummarySize = conf.getInt(MacroBaseConf.OUTLIER_ITEM_SUMMARY_SIZE,
                                             MacroBaseDefaults.OUTLIER_ITEM_SUMMARY_SIZE);
        inlierItemSummarySize = conf.getInt(MacroBaseConf.INLIER_ITEM_SUMMARY_SIZE,
                                            MacroBaseDefaults.INLIER_ITEM_SUMMARY_SIZE);

        startSemaphore = new Semaphore(0);
        endSemaphore = new Semaphore(0);
    }

    class RunnableStreamingAnalysis implements Runnable {
        List<Datum> data;
        DatumEncoder encoder;
        OutlierDetector detector;

        List<ItemsetResult> itemsetResults;

        ExponentiallyBiasedAChao<Datum> inputReservoir;
        ExponentiallyBiasedAChao<Double> scoreReservoir;
        ExponentiallyDecayingEmergingItemsets streamingSummarizer;
        AbstractPeriodicUpdater analysisUpdater;

        RunnableStreamingAnalysis(List<Datum> data, DatumEncoder encoder,
                                  OutlierDetector detector) {
            this.data = data;
            this.encoder = encoder;

            this.detector = detector;

            inputReservoir =
                    new ExponentiallyBiasedAChao<>(inputReservoirSize, decayRate);

            if (randomSeed != null) {
                inputReservoir.setSeed(randomSeed);
            }

            scoreReservoir = null;
            if (forceUsePercentile) {
                scoreReservoir = new ExponentiallyBiasedAChao<>(scoreReservoirSize, decayRate);
                if (randomSeed != null) {
                    scoreReservoir.setSeed(randomSeed);
                }
            }

            if (useRealTimePeriod) {
                analysisUpdater = new WallClockAnalysisDecayer(System.currentTimeMillis(),
                        summaryPeriod,
                        inputReservoir,
                        scoreReservoir,
                        detector,
                        streamingSummarizer);
            } else {
                analysisUpdater = new TupleAnalysisDecayer(summaryPeriod,
                        inputReservoir,
                        scoreReservoir,
                        detector,
                        streamingSummarizer);
            }

            streamingSummarizer =
                    new ExponentiallyDecayingEmergingItemsets(inlierItemSummarySize,
                            outlierItemSummarySize,
                            minSupport,
                            minOIRatio,
                            decayRate,
                            attributes.size());
        }

        public List<ItemsetResult> getItemsetResults() {
            return itemsetResults;
        }

        @Override
        public void run() {
            try {
                startSemaphore.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException("Start semaphore interrupted");
            }

            int tupleNo = 0;
            List<Double> scoreReservoirCopy = new ArrayList<Double> ();
            for (int run = 0; run < numRuns; run++) {
                for (Datum d : data) {
                    inputReservoir.insert(d);
                    if (tupleNo == warmupCount) {
                        for (Datum id : inputReservoir.getReservoir()) {
                            scoreReservoir.insert(detector.score(id));
                        }
                        scoreReservoirCopy = Lists.newArrayList(scoreReservoir.getReservoir());
                        Collections.sort(scoreReservoirCopy);
                    } else if (tupleNo > warmupCount) {
                        long now = useRealTimePeriod ? System.currentTimeMillis() : 0;
                        if (analysisUpdater.updateIfNecessary(now, tupleNo)) {
                            scoreReservoirCopy = Lists.newArrayList(scoreReservoir.getReservoir());
                            Collections.sort(scoreReservoirCopy);
                        }
                        double score = detector.score(d);

                        if (scoreReservoir != null) {
                            scoreReservoir.insert(score);
                        }

                        if ((forceUseZScore && detector.isZScoreOutlier(score, zScore)) ||
                                forceUsePercentile && detector.isPercentileOutlier(score,
                                        targetPercentile, scoreReservoirCopy)) {
                            streamingSummarizer.markOutlier(d);
                        } else {
                            streamingSummarizer.markInlier(d);
                        }
                    }
                }
                tupleNo++;
            }

            itemsetResults = streamingSummarizer.getItemsets(encoder);
            endSemaphore.release();
        }
    }

    public AnalysisResult analyzeOnePass() throws SQLException, IOException, ConfigurationException, InterruptedException {
        DatumEncoder encoder = new DatumEncoder();
        DataLoader loader = constructLoader();

        OutlierDetector detector = constructDetector(randomSeed);

        ExponentiallyBiasedAChao<Datum> inputReservoir =
                new ExponentiallyBiasedAChao<>(inputReservoirSize, decayRate);

        // Score reservoir in main thread used ONLY if numThreads = 1
        ExponentiallyBiasedAChao<Double> scoreReservoir = null;

        if (forceUsePercentile) {
            scoreReservoir = new ExponentiallyBiasedAChao<>(scoreReservoirSize, decayRate);
            if (randomSeed != null) {
                scoreReservoir.setSeed(randomSeed);
            }
        }

        // Streaming summarizer in main thread used ONLY if numThreads = 1
        ExponentiallyDecayingEmergingItemsets streamingSummarizer =
                new ExponentiallyDecayingEmergingItemsets(inlierItemSummarySize,
                        outlierItemSummarySize,
                        minSupport,
                        minOIRatio,
                        decayRate,
                        attributes.size());

        // Analysis updated in main thread used ONLY if numThreads = 1
        AbstractPeriodicUpdater analysisUpdater;
        if (useRealTimePeriod) {
            analysisUpdater = new WallClockAnalysisDecayer(System.currentTimeMillis(),
                    summaryPeriod,
                    inputReservoir,
                    scoreReservoir,
                    detector,
                    streamingSummarizer);
        } else {
            analysisUpdater = new TupleAnalysisDecayer(summaryPeriod,
                    inputReservoir,
                    scoreReservoir,
                    detector,
                    streamingSummarizer);
        }

        AbstractPeriodicUpdater modelUpdater;
        if (useRealTimePeriod) {
            modelUpdater = new WallClockRetrainer(System.currentTimeMillis(),
                    modelRefreshPeriod,
                    inputReservoir,
                    detector);
        } else {
            modelUpdater = new TupleBasedRetrainer(modelRefreshPeriod,
                    inputReservoir,
                    detector);
        }

        Stopwatch tsw = Stopwatch.createUnstarted();

        List<Datum> data = loader.getData(encoder);
        if (randomSeed == null) {
            Collections.shuffle(data);
        } else {
            Collections.shuffle(data, new Random(randomSeed));
        }

        List<List<Datum>> partitionedData = new ArrayList<List<Datum>>();
        if (numThreads > 1) {
            for (int i = 0; i < numThreads - 1; i++) {
                partitionedData.add(new ArrayList<Datum>());
            }

            for (int i = 0; i < data.size(); i++) {
                partitionedData.get(i % (numThreads - 1)).add(data.get(i));
            }
        }

        // Want to measure time taken once data is loaded
        tsw.start();

        List<RunnableStreamingAnalysis> rsas = new ArrayList<RunnableStreamingAnalysis>();
        // Run per-core detection and summarization
        for (int i = 0; i < numThreads - 1; i++) {
            RunnableStreamingAnalysis rsa = new RunnableStreamingAnalysis(partitionedData.get(i), encoder, detector);
            rsas.add(rsa);
            Thread t = new Thread(rsa);
            t.start();
        }

        List<Double> scoreReservoirCopy = new ArrayList<Double>();

        int tupleNo = 0;

        // TODO: If numThreads = 1, only want to make numRuns passes; otherwise loop indefinitely until all helper
        // threads are done with training
        outerLoop:
        for (int run = 0; ; run++) {
            for (Datum d : data) {
                inputReservoir.insert(d);

                if (tupleNo == warmupCount) {
                    detector.train(inputReservoir.getReservoir());
                    // Start semaphore to kick off all threads
                    startSemaphore.release(numThreads - 1);
                    // Only score on this thread if there are no helper threads
                    if (numThreads == 1) {
                        for (Datum id : inputReservoir.getReservoir()) {
                            scoreReservoir.insert(detector.score(id));
                        }
                        scoreReservoirCopy = Lists.newArrayList(scoreReservoir.getReservoir());
                        Collections.sort(scoreReservoirCopy);
                    }
                } else if (tupleNo >= warmupCount) {
                    long now = useRealTimePeriod ? System.currentTimeMillis() : 0;

                    modelUpdater.updateIfNecessary(now, tupleNo);

                    // Only score on this thread if there are no helper threads
                    if (numThreads == 1) {
                        if (analysisUpdater.updateIfNecessary(now, tupleNo)) {
                            scoreReservoirCopy = Lists.newArrayList(scoreReservoir.getReservoir());
                            Collections.sort(scoreReservoirCopy);
                        }
                        double score = detector.score(d);

                        if (scoreReservoir != null) {
                            scoreReservoir.insert(score);
                        }

                        if ((forceUseZScore && detector.isZScoreOutlier(score, zScore)) ||
                                forceUsePercentile && detector.isPercentileOutlier(score,
                                        targetPercentile, scoreReservoirCopy)) {
                            streamingSummarizer.markOutlier(d);
                        } else {
                            streamingSummarizer.markInlier(d);
                        }
                    }
                }

                tupleNo += 1;
            }

            if ((numThreads > 1 && endSemaphore.tryAcquire(numThreads - 1)) ||
                    (numThreads == 1 && run >= numRuns)) {
                // Hack to easily break out of nested loop
                break outerLoop;
            }
        }

        // TODO: Update this as necessary
        List<ItemsetResult> itemsetResults = new ArrayList<ItemsetResult>();
        if (numThreads == 1) {
            itemsetResults = streamingSummarizer.getItemsets(encoder);
        } else {
            for (int i = 0; i < numThreads-1; i++) {
                for (ItemsetResult itemsetResult : rsas.get(i).getItemsetResults()) {
                    itemsetResults.add(itemsetResult);
                }
            }
        }

        // Stop timer as soon as all itemsets have been mined and aggregated
        tsw.stop();

        double tuplesPerSecond = ((double) data.size()) / ((double) tsw.elapsed(TimeUnit.MICROSECONDS));
        tuplesPerSecond *= (1000000 * numRuns);

        log.debug("Tuples / second = {} tuples / second", tuplesPerSecond);
        log.debug("Number of itemsets: {}", itemsetResults.size());

        // todo: refactor this so we don't just return zero
        return new AnalysisResult(0, 0, 0, 0, 0, itemsetResults);
    }
}
