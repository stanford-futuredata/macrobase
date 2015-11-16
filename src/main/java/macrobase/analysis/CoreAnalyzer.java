package macrobase.analysis;

import macrobase.analysis.outlier.result.DatumWithScore;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.outlier.ZScoreDetector;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.count.ExactCount;
import macrobase.analysis.summary.itemset.FPGrowth;
import macrobase.datamodel.Datum;
import macrobase.ingest.DatumEncoder;
import macrobase.ingest.PostgresLoader;

import java.sql.SQLException;
import java.util.*;

public class CoreAnalyzer {

    private static final double ZSCORE = 3;
    private static final double MIN_SUPPORT = 0.001;
    private static final double MIN_INLIER_RATIO = 1;

    public static AnalysisResult analyze(PostgresLoader loader,
                                         List<String> attributes,
                                         List<String> lowMetrics,
                                         List<String> highMetrics,
                                         String baseQuery) throws SQLException {
        DatumEncoder encoder = new DatumEncoder();

        // OUTLIER ANALYSIS

        List<Datum> data = loader.getData(encoder,
                                          attributes,
                                          lowMetrics,
                                          highMetrics,
                                          baseQuery);

        ZScoreDetector detector = new ZScoreDetector(ZSCORE);
        OutlierDetector.BatchResult or = detector.classifyBatch(data);

        // SUMMARY

        final int supportCountRequired = (int) MIN_SUPPORT*or.getOutliers().size();

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
                        outlierInlierRatio = (((double) outlierCount) / inlierCount)*or.getOutliers().size()/or.getInliers().size();
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
        fpg.getItemsets(outlierTransactions, MIN_SUPPORT);

        return null;
    }
}
