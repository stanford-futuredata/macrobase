package macrobase.analysis;

import com.google.common.base.Stopwatch;
import macrobase.analysis.outlier.MinCovDet;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.outlier.ZScore;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.itemset.FPGrowthEmerging;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import macrobase.ingest.PostgresLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CoreAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(CoreAnalyzer.class);

    private static final double ZSCORE = 3;
    private static final double MIN_SUPPORT = 0.001;
    private static final double MIN_INLIER_RATIO = 1;

    public static AnalysisResult analyze(PostgresLoader loader,
                                         List<String> attributes,
                                         List<String> lowMetrics,
                                         List<String> highMetrics,
                                         String baseQuery) throws SQLException {
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

        log.debug("Starting classification...");
        sw.start();
        OutlierDetector detector;
        if(lowMetrics.size() + highMetrics.size() == 1) {
            detector = new ZScore(ZSCORE);
        } else {
            detector = new MinCovDet(.01);
        }
        OutlierDetector.BatchResult or = detector.classifyBatch(data);

        sw.stop();

        long classifyTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

        log.debug("...ended classification (time: {}ms)!", classifyTime);

        // SUMMARY

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
        sw.reset();
        long summarizeTime = sw.elapsed(TimeUnit.MILLISECONDS);
        log.debug("...ended summarization (time: {}ms)!", summarizeTime);

        return new AnalysisResult(outlierSize, inlierSize, loadTime, classifyTime, summarizeTime, isr);
    }
}
