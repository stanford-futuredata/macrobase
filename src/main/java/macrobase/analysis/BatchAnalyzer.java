package macrobase.analysis;

import com.google.common.base.Stopwatch;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataLoader;
import macrobase.ingest.DatumEncoder;
import macrobase.ingest.SQLLoader;

import macrobase.runtime.standalone.BaseStandaloneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BatchAnalyzer extends BaseAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(BatchAnalyzer.class);

    public BatchAnalyzer() {
        super();
    }

    public BatchAnalyzer(BaseStandaloneConfiguration configuration) {
        super(configuration);
    }

    public AnalysisResult analyze(DataLoader loader,
                                  List<String> attributes,
                                  List<String> lowMetrics,
                                  List<String> highMetrics,
                                  String baseQuery) throws SQLException, IOException {
        DatumEncoder encoder = new DatumEncoder();

        Stopwatch sw = Stopwatch.createUnstarted();

        // OUTLIER ANALYSIS

        log.debug("Starting loading...");
        sw.start();
        List<Datum> data = loader.getData(encoder,
                                          attributes,
                                          lowMetrics,
                                          highMetrics,
                                          baseQuery);
        sw.stop();

        long loadTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

        log.debug("...ended loading (time: {}ms)!", loadTime);
        
        Stopwatch tsw = Stopwatch.createUnstarted();
        Stopwatch tsw2 = Stopwatch.createUnstarted();
        tsw.start();
        tsw2.start();

        sw.start();
        int metricsDimensions = lowMetrics.size() + highMetrics.size();
        OutlierDetector detector = constructDetector(metricsDimensions);

        if (serverConfiguration != null && serverConfiguration.getStoreScoreDistribution() != null ) {
            detector.storeScoresInFile(serverConfiguration.getStoreScoreDistribution());
        }

        OutlierDetector.BatchResult or;
        if(forceUsePercentile || (!forceUseZScore && TARGET_PERCENTILE > 0)) {
            or = detector.classifyBatchByPercentile(data, TARGET_PERCENTILE);
        } else {
            or = detector.classifyBatchByZScoreEquivalent(data, ZSCORE);
        }
        sw.stop();

        long classifyTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

        // Dump scored results into a file if requested.
        if (serverConfiguration != null && serverConfiguration.getStoreScoreDistribution() != null) {

            Gson gson = new GsonBuilder()
                    .enableComplexMapKeySerialization()
                    .serializeNulls()
                    .setPrettyPrinting()
                    .setVersion(1.0)
                    .create();
            final File dir = new File("target/scores");
            dir.mkdirs();
            try (PrintStream out = new PrintStream(new File(dir, serverConfiguration.getStoreScoreDistribution()), "UTF-8")) {
                out.println(gson.toJson(or));
            }

            try (PrintStream out = new PrintStream(new File(dir, "outliers_" + serverConfiguration.getStoreScoreDistribution()), "UTF-8")) {
                out.println(gson.toJson(or.getOutliers()));
            }
            try (PrintStream out = new PrintStream(new File(dir, "inliers_" + serverConfiguration.getStoreScoreDistribution()), "UTF-8")) {
                out.println(gson.toJson(or.getInliers()));
            }
        }

        tsw2.stop();

        // SUMMARY

        @SuppressWarnings("unused")
		final int supportCountRequired = (int) MIN_SUPPORT*or.getOutliers().size();

        final int inlierSize = or.getInliers().size();
        final int outlierSize = or.getOutliers().size();

        log.debug("Starting summarization...");

        sw.start();
        FPGrowthEmerging fpg = new FPGrowthEmerging();
        List<ItemsetResult> isr = fpg.getEmergingItemsetsWithMinSupport(or.getInliers(),
                                                                        or.getOutliers(),
                                                                        MIN_SUPPORT,
                                                                        MIN_INLIER_RATIO,
                                                                        encoder);
        sw.stop();
        tsw.stop();
        
        double tuplesPerSecond = ((double) data.size()) / ((double) tsw.elapsed(TimeUnit.MICROSECONDS));
        tuplesPerSecond *= 1000000;
        
        long summarizeTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
        log.debug("...ended summarization (time: {}ms)!", summarizeTime);

        log.debug("Number of itemsets: {}", isr.size());
        log.debug("...ended total (time: {}ms)!", (tsw.elapsed(TimeUnit.MICROSECONDS) / 1000) + 1);
        log.debug("Tuples / second = {} tuples / second", tuplesPerSecond);

        tuplesPerSecond = ((double) data.size()) / ((double) tsw2.elapsed(TimeUnit.MICROSECONDS));
        tuplesPerSecond *= 1000000;
        log.debug("Tuples / second w/o itemset mining = {} tuples / second", tuplesPerSecond);

        return new AnalysisResult(outlierSize, inlierSize, loadTime, classifyTime, summarizeTime, isr);
    }
}
