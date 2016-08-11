package macrobase.analysis.stats;

import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class DetectorTest {
    private final Integer INLIER_SCORE = 0;
    private final Integer OUTLIER_SCORE = 1000;


    private class DummyDetector extends BatchTrainScore {
        public DummyDetector(MacroBaseConf conf) {
            super(conf);
        }

        @Override
        public void train(List<Datum> data) { }

        @Override
        public double score(Datum datum) {
            if(datum.getMetrics().getEntry(0) == 0) {
                return INLIER_SCORE;
            } else {
                return OUTLIER_SCORE;
            }
        }

        @Override
        public double getZScoreEquivalent(double zscore) {
            return 3;
        }
    }

    final int NUM_OUTLIERS = 10;
    final int NUM_INLIERS = 90;
    final double PERCENTILE_THRESH = ((double)NUM_INLIERS)/(NUM_INLIERS + NUM_OUTLIERS);

    private List<Datum> generateTestData() {
        List<Datum> ret = new ArrayList<>();

        for (int i = 0; i < NUM_INLIERS; ++i) {
            RealVector vec = new ArrayRealVector(1);
            vec.set(0);
            ret.add(new Datum(new ArrayList<>(), vec));
        }

        for (int i = 0; i < NUM_OUTLIERS; ++i) {
            RealVector vec = new ArrayRealVector(1);
            vec.set(1);
            ret.add(new Datum(new ArrayList<>(), vec));
        }

        return ret;
    }

    @Test
    public void testThresholdScoring() {
        List<Datum> data = generateTestData();

        DummyDetector detector = new DummyDetector(new MacroBaseConf());
        BatchTrainScore.BatchResult or = detector.classifyBatchByPercentile(data, PERCENTILE_THRESH);
        for (DatumWithScore d : or.getInliers()) {
            assertEquals(0, d.getDatum().getMetrics().getEntry(0), 0);
            assertEquals(INLIER_SCORE, d.getScore(), 0);
        }

        for (DatumWithScore d : or.getOutliers()) {
            assertEquals(1, d.getDatum().getMetrics().getEntry(0), 0);
            assertEquals(OUTLIER_SCORE, d.getScore(), 0);
        }

        assertEquals(or.getInliers().size(), NUM_INLIERS);
        assertEquals(or.getOutliers().size(), NUM_OUTLIERS);
    }

    @Test
    public void testZScoreEquivalent() {
        List<Datum> data = generateTestData();

        DummyDetector detector = new DummyDetector(new MacroBaseConf());
        BatchTrainScore.BatchResult or = detector.classifyBatchByZScoreEquivalent(data, 3);

        for (DatumWithScore d : or.getInliers()) {
            assertEquals(0, d.getDatum().getMetrics().getEntry(0), 0);
            assertEquals(INLIER_SCORE, d.getScore(), 0);
        }

        for (DatumWithScore d : or.getOutliers()) {
            assertEquals(1, d.getDatum().getMetrics().getEntry(0), 0);
            assertEquals(OUTLIER_SCORE, d.getScore(), 0);
        }

        assertEquals(or.getInliers().size(), NUM_INLIERS);
        assertEquals(or.getOutliers().size(), NUM_OUTLIERS);
    }
}
