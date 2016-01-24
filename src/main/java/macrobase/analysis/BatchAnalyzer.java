package macrobase.analysis;

import com.google.common.base.Stopwatch;

import macrobase.analysis.outlier.MAD;
import macrobase.analysis.outlier.MinCovDet;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.outlier.ZScore;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import macrobase.ingest.SQLLoader;

import macrobase.runtime.standalone.BaseStandaloneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BatchAnalyzer extends BaseAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(BatchAnalyzer.class);

    public AnalysisResult analyze(SQLLoader loader,
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
        tsw.start();

        sw.start();
        int metricsDimensions = lowMetrics.size() + highMetrics.size();
        OutlierDetector detector = constructDetector(metricsDimensions);

        OutlierDetector.BatchResult or;
        if(forceUsePercentile || (!forceUseZScore && TARGET_PERCENTILE > 0)) {
            or = detector.classifyBatchByPercentile(data, TARGET_PERCENTILE);
        } else {
            or = detector.classifyBatchByZScoreEquivalent(data, ZSCORE);
        }
        sw.stop();

        long classifyTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

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

        return new AnalysisResult(outlierSize, inlierSize, loadTime, classifyTime, summarizeTime, isr);
    }
}
