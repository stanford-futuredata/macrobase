package macrobase.analysis;

import com.google.common.base.Stopwatch;

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

        private long trainingAndScoringTime;
        private long miningTime;

        List<ItemsetResult> itemsetResults;

        RunnableStreamingAnalysis(List<Datum> data, DatumEncoder encoder) {
            this.data = data;
            this.encoder = encoder;

            trainingAndScoringTime = 0;
            miningTime = 0;
        }

        public List<ItemsetResult> getItemsetResults() {
            return itemsetResults;
        }

        public long getTrainingAndScoringTime() {
            return trainingAndScoringTime;
        }

        public long getMiningTime() {
            return miningTime;
        }

        @Override
        public void run() {
            try {
                startSemaphore.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException("Start semaphore interrupted");
            }

            OutlierDetector detector = constructDetector(randomSeed);

            ExponentiallyBiasedAChao<Datum> inputReservoir =
                    new ExponentiallyBiasedAChao<>(inputReservoirSize, decayRate);

            if (randomSeed != null) {
                inputReservoir.setSeed(randomSeed);
            }

            ExponentiallyBiasedAChao<Double> scoreReservoir = null;

            if (forceUsePercentile) {
                scoreReservoir = new ExponentiallyBiasedAChao<>(scoreReservoirSize, decayRate);
                if (randomSeed != null) {
                    scoreReservoir.setSeed(randomSeed);
                }
            }

            ExponentiallyDecayingEmergingItemsets streamingSummarizer =
                    new ExponentiallyDecayingEmergingItemsets(inlierItemSummarySize,
                            outlierItemSummarySize,
                            minSupport,
                            minOIRatio,
                            decayRate,
                            attributes.size());

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
                        detector,
                        streamingSummarizer);
            } else {
                modelUpdater = new TupleBasedRetrainer(modelRefreshPeriod,
                        inputReservoir,
                        detector,
                        streamingSummarizer);
            }

            int tupleNo = 0;

            Stopwatch sw = Stopwatch.createUnstarted();
            sw.start();

            for (int run = 0; run < numRuns; run++) {
                for (Datum d : data) {
                    inputReservoir.insert(d);

                    if (tupleNo == warmupCount) {
                        detector.train(inputReservoir.getReservoir());
                        for (Datum id : inputReservoir.getReservoir()) {
                            scoreReservoir.insert(detector.score(id));
                        }
                        detector.updateRecentScoreList(scoreReservoir.getReservoir());
                    } else if (tupleNo >= warmupCount) {
                        long now = useRealTimePeriod ? System.currentTimeMillis() : 0;

                        analysisUpdater.updateIfNecessary(now, tupleNo);
                        modelUpdater.updateIfNecessary(now, tupleNo);
                        double score = detector.score(d);

                        if (scoreReservoir != null) {
                            scoreReservoir.insert(score);
                        }

                        if ((forceUseZScore && detector.isZScoreOutlier(score, zScore)) ||
                                forceUsePercentile && detector.isPercentileOutlier(score,
                                        targetPercentile)) {
                            streamingSummarizer.markOutlier(d);
                        } else {
                            streamingSummarizer.markInlier(d);
                        }
                    }

                    tupleNo += 1;
                }
            }

            sw.stop();
            trainingAndScoringTime = sw.elapsed(TimeUnit.MICROSECONDS);

            sw.reset();
            sw.start();
            itemsetResults = streamingSummarizer.getItemsets(encoder);
            sw.stop();
            miningTime = sw.elapsed(TimeUnit.MICROSECONDS);

            endSemaphore.release();
        }
    }

    public AnalysisResult analyzeOnePass() throws SQLException, IOException, ConfigurationException, InterruptedException {
        DatumEncoder encoder = new DatumEncoder();
        DataLoader loader = constructLoader();

        Stopwatch tsw = Stopwatch.createUnstarted();

        List<Datum> data = loader.getData(encoder);
        if (randomSeed == null) {
            Collections.shuffle(data);
        } else {
            Collections.shuffle(data, new Random(randomSeed));
        }

        List<List<Datum>> partitionedData = new ArrayList<List<Datum>>();
        for (int i = 0; i < numThreads; i++) {
            partitionedData.add(new ArrayList<Datum>());
        }

        for (int i = 0; i < data.size(); i++) {
            partitionedData.get(i % numThreads).add(data.get(i));
        }

        // Want to measure time taken once data is loaded
        tsw.start();

        List<RunnableStreamingAnalysis> rsas = new ArrayList<RunnableStreamingAnalysis>();
        // Run per-core detection and summarization
        for (int i = 0; i < numThreads; i++) {
            RunnableStreamingAnalysis rsa = new RunnableStreamingAnalysis(partitionedData.get(i), encoder);
            rsas.add(rsa);
            Thread t = new Thread(rsa);
            t.start();
        }

        // Start semaphore to kick off all threads
        startSemaphore.release(numThreads);
        // Stall until all threads are done
        endSemaphore.acquire(numThreads);

        long avgTrainingAndScoringTime = 0;
        long avgMiningTime = 0;
        for (int i = 0; i < numThreads; i++) {
            avgTrainingAndScoringTime += rsas.get(i).getTrainingAndScoringTime();
            avgMiningTime += rsas.get(i).getMiningTime();
        }

        avgTrainingAndScoringTime /= numThreads;
        avgMiningTime /= numThreads;

        List<ItemsetResult> itemsetResults = new ArrayList<ItemsetResult>();
        for (int i = 0; i < numThreads; i++) {
            for (ItemsetResult itemsetResult : rsas.get(i).getItemsetResults()) {
                itemsetResults.add(itemsetResult);
            }
        }

        // Stop timer as soon as all itemsets have been mined and aggregated
        tsw.stop();

        double tuplesPerSecond = ((double) data.size()) / ((double) tsw.elapsed(TimeUnit.MICROSECONDS));
        tuplesPerSecond *= (1000000 * numRuns);

        log.debug("Tuples / second = {} tuples / second", tuplesPerSecond);
        log.debug("Number of itemsets: {}", itemsetResults.size());
        log.debug("..ended training and scoring (time: {}ms)!", avgTrainingAndScoringTime);
        log.debug("...ended mining (time: {}ms)!", avgMiningTime);

        // todo: refactor this so we don't just return zero
        return new AnalysisResult(0, 0, 0, 0, 0, itemsetResults);
    }
}
