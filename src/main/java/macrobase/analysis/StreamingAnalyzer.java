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
import java.util.Collections;
import java.util.List;
import java.util.Random;
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
    }

    public AnalysisResult analyzeOnePass() throws SQLException, IOException, ConfigurationException {
        DatumEncoder encoder = new DatumEncoder();

        Stopwatch sw = Stopwatch.createUnstarted();
        Stopwatch tsw = Stopwatch.createUnstarted();
        Stopwatch tsw2 = Stopwatch.createUnstarted();

        // OUTLIER ANALYSIS

        DataLoader loader = constructLoader();

        log.debug("Starting loading...");
        sw.start();
        List<Datum> data = loader.getData(encoder);

        if (randomSeed == null) {
            Collections.shuffle(data);
        } else {
            Collections.shuffle(data, new Random(randomSeed));
        }

        sw.stop();

        long loadTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

        log.debug("...ended loading (time: {}ms)!", loadTime);

        //System.console().readLine("waiting to start (press a key)");
        tsw.start();
        tsw2.start();

        OutlierDetector detector = constructDetector(randomSeed);

        ExponentiallyBiasedAChao<Datum> inputReservoir =
                new ExponentiallyBiasedAChao<>(inputReservoirSize, decayRate);

        if(randomSeed != null) {
            inputReservoir.setSeed(randomSeed);
        }

        ExponentiallyBiasedAChao<Double> scoreReservoir = null;

        if(forceUsePercentile) {
            scoreReservoir = new ExponentiallyBiasedAChao<>(scoreReservoirSize, decayRate);
            if(randomSeed != null) {
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
        if(useRealTimePeriod) {
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
        if(useRealTimePeriod) {
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
        long totSummarizationTime = 0;

        for(Datum d: data) {
            inputReservoir.insert(d);

            if(tupleNo == warmupCount) {
            	sw.start();
                detector.train(inputReservoir.getReservoir());
                for(Datum id : inputReservoir.getReservoir()) {
                    scoreReservoir.insert(detector.score(id));
                }
                detector.updateRecentScoreList(scoreReservoir.getReservoir());
                sw.stop();
                sw.reset();
                log.debug("...ended warmup training (time: {}ms)!", sw.elapsed(TimeUnit.MILLISECONDS));
            } else if(tupleNo >= warmupCount) {
                long now = useRealTimePeriod ? System.currentTimeMillis() : 0;

                analysisUpdater.updateIfNecessary(now, tupleNo);
                modelUpdater.updateIfNecessary(now, tupleNo);
                double score = detector.score(d);

                if(scoreReservoir != null) {
                    scoreReservoir.insert(score);
                }

                if((forceUseZScore && detector.isZScoreOutlier(score, zScore)) ||
                   forceUsePercentile && detector.isPercentileOutlier(score,
                                                                      targetPercentile)) {
                    streamingSummarizer.markOutlier(d);
                } else {
                    streamingSummarizer.markInlier(d);
                }
            }

            tupleNo += 1;
        }
        tsw2.stop();

        sw.start();
        List<ItemsetResult> isr = streamingSummarizer.getItemsets(encoder);
        sw.stop();
        totSummarizationTime += sw.elapsed(TimeUnit.MICROSECONDS);
        sw.reset();
        tsw.stop();
        
        double tuplesPerSecond = ((double) data.size()) / ((double) tsw.elapsed(TimeUnit.MICROSECONDS));
        tuplesPerSecond *= 1000000;

        log.debug("...ended summarization (time: {}ms)!", (totSummarizationTime / 1000) + 1);
        log.debug("...ended total (time: {}ms)!", (tsw.elapsed(TimeUnit.MICROSECONDS) / 1000) + 1);
        log.debug("Tuples / second = {} tuples / second", tuplesPerSecond);

        tuplesPerSecond = ((double) data.size()) / ((double) tsw2.elapsed(TimeUnit.MICROSECONDS));
        tuplesPerSecond *= 1000000;
        log.debug("Tuples / second w/o itemset mining = {} tuples / second", tuplesPerSecond);

        log.debug("Number of itemsets: {}", isr.size());

        // todo: refactor this so we don't just return zero
        return new AnalysisResult(0, 0, loadTime, 0, totSummarizationTime, isr);
    }
}
