package macrobase.analysis.stats;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.datamodel.HasMetrics;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class for All outlier detection algorithms
 */
public abstract class BatchTrainScore {
    private static final Logger log = LoggerFactory.getLogger(BatchTrainScore.class);

    public class BatchResult {
        private List<DatumWithScore> inliers;
        private List<DatumWithScore> outliers;

        public BatchResult(List<DatumWithScore> inliers, List<DatumWithScore> outliers) {
            this.inliers = inliers;
            this.outliers = outliers;
        }

        public List<DatumWithScore> getInliers() {
            return inliers;
        }

        public List<DatumWithScore> getOutliers() {
            return outliers;
        }
    }
    
    // Constructor with this signature should be implemented by subclasses
    public BatchTrainScore(MacroBaseConf conf) {}

    public abstract void train(List<Datum> data);

    public abstract double score(Datum datum);

    @Deprecated
    public abstract double getZScoreEquivalent(double zscore);

    private Map<Double, Double> cachedZScoreEquivalents = new HashMap<>();

    private List<Double> recentScoresSorted;

    private List<DatumWithScore> scoreBatch(List<Datum> data) {
        List<DatumWithScore> ret = new ArrayList<>();
        for (Datum d : data) {
            ret.add(new DatumWithScore(d, score(d)));
        }
        return ret;
    }

    @Deprecated
    public BatchResult classifyBatchByPercentile(List<Datum> data,
                                                 double percentile) {
        Stopwatch sw = Stopwatch.createUnstarted();
        log.debug("Starting training...");
        sw.start();
        train(data);
        sw.stop();
        long trainTime = sw.elapsed(TimeUnit.MILLISECONDS);
        log.debug("...ended training (time: {}ms)!", trainTime);

        log.debug("Starting scoring...");
        sw.reset();
        sw.start();
        int splitPoint = (int) (data.size() * percentile);
        List<DatumWithScore> scoredData = scoreBatch(data);

        scoredData.sort((a, b) -> a.getScore().compareTo(b.getScore()));
        sw.stop();
        long scoringTime = sw.elapsed(TimeUnit.MILLISECONDS);
        log.debug("...ended scoring (time: {}ms)!", scoringTime);

        return new BatchResult(scoredData.subList(0, splitPoint),
                               scoredData.subList(splitPoint, scoredData.size()));

    }

    @Deprecated
    public BatchResult classifyBatchByZScoreEquivalent(List<Datum> data,
                                                       double zscore) {

        List<DatumWithScore> inliers = new ArrayList<>();
        List<DatumWithScore> outliers = new ArrayList<>();

        Stopwatch sw = Stopwatch.createUnstarted();
        log.debug("Starting training...");
        sw.start();
        train(data);
        sw.stop();
        long trainTime = sw.elapsed(TimeUnit.MILLISECONDS);
        log.debug("...ended training (time: {}ms)!", trainTime);

        log.debug("Starting scoring...");
        sw.reset();
        sw.start();

        double thresh = cachedZScoreEquivalents.computeIfAbsent(zscore, k -> getZScoreEquivalent(zscore));

        // take an efficiency hit for modularity :(
        for (Datum d : data) {
            double dScore = score(d);
            DatumWithScore dws = new DatumWithScore(d, dScore);
            if (dScore >= thresh) {
                outliers.add(dws);
            } else {
                inliers.add(dws);
            }
        }
        sw.stop();
        long scoringTime = sw.elapsed(TimeUnit.MILLISECONDS);
        log.debug("...ended scoring (time: {}ms)!", scoringTime);
        return new BatchResult(inliers, outliers);
    }

    protected RealMatrix getCovariance(List<? extends HasMetrics> data) {
        int rank = data.get(0).getMetrics().getDimension();
        RealMatrix ret = new Array2DRowRealMatrix(data.size(), rank);
        int index = 0;
        for (HasMetrics d : data) {
            ret.setRow(index, d.getMetrics().toArray());
            index += 1;
        }

        return (new Covariance(ret)).getCovarianceMatrix();
    }
}
