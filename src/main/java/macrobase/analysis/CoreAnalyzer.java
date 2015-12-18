package macrobase.analysis;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import macrobase.analysis.outlier.result.DatumWithScore;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.outlier.ZScoreDetector;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.count.ExactCount;
import macrobase.analysis.summary.itemset.FPGrowth;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.analysis.summary.itemset.result.ItemsetWithCount;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import macrobase.ingest.PostgresLoader;
import macrobase.ingest.result.ColumnValue;
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
        ZScoreDetector detector = new ZScoreDetector(ZSCORE);
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
        Map<Integer, Integer> inlierCounts = new ExactCount().count(or.getInliers()).getCounts();
        Map<Integer, Integer> outlierCounts = new ExactCount().count(or.getOutliers()).getCounts();


        // TODO: truncate inliers!
        ArrayList<Set<Integer>> outlierTransactions = new ArrayList<>();

        for(DatumWithScore d : or.getOutliers()) {
            Set<Integer> txn = null;

            for(int i : d.getDatum().getAttributes()) {
                int outlierCount = outlierCounts.get(i);
                if(outlierCount > supportCountRequired) {
                    Integer inlierCount = inlierCounts.get(i);

                    double outlierInlierRatio;
                    if(inlierCount == null || inlierCount == 0) {
                        outlierInlierRatio = Double.POSITIVE_INFINITY;
                    } else {
                        outlierInlierRatio = ((double)outlierCount/outlierSize)/((double)inlierCount/inlierSize);
                    }
                    if(outlierInlierRatio > MIN_INLIER_RATIO) {
                        if(txn == null) {
                            txn = new HashSet<>();
                        }
                        txn.add(i);
                    }
                }
            }

            if(txn != null) {
                outlierTransactions.add(txn);
            }
        }

        FPGrowth fpg = new FPGrowth();
        List<ItemsetWithCount> iwc = fpg.getItemsets(outlierTransactions, MIN_SUPPORT);
        sw.stop();

        long summarizeTime = sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();

        log.debug("...ended summarization (time: {}ms)!", summarizeTime);


        // to fix!
        final double INLIER_RATIO = 0;

        iwc.sort((x, y) -> x.getCount() != y.getCount() ?
                -Integer.compare(x.getCount(), y.getCount()) :
                -Integer.compare(x.getItems().size(), y.getItems().size()));

        List<ItemsetResult> isr = new ArrayList<>();

        Set<Integer> prevSet = null;
        Integer prevCount = -1;
        for(ItemsetWithCount i : iwc) {
            if(i.getCount() == prevCount) {
                if(prevSet != null && Sets.difference(i.getItems(), prevSet).size() == 0) {
                    continue;
                }
            }

            prevCount = i.getCount();
            prevSet = i.getItems();

            List<ColumnValue> cols = new ArrayList<>();

            for(int item : i.getItems()) {
                cols.add(encoder.getAttribute(item));
            }

            isr.add(new ItemsetResult((double) i.getCount()/outlierSize,
                                      i.getCount(),
                                      INLIER_RATIO,
                                      cols));
        }

        return new AnalysisResult(outlierSize, inlierSize, loadTime, classifyTime, summarizeTime, isr);
    }
}
