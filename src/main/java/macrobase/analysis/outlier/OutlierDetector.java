package macrobase.analysis.outlier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;

/**
 * Created by pbailis on 12/14/15.
 */
public abstract class OutlierDetector {
	private static final Logger log = LoggerFactory.getLogger(OutlierDetector.class);
	
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

    public abstract void train(List<Datum> data);
    public abstract double score(Datum datum);
    public abstract double getZScoreEquivalent(double zscore);

    private Map<Double, Double> cachedZScoreEquivalents = new HashMap<>();

    public boolean isZScoreOutlier(double score, double zscore) {
        double thresh = cachedZScoreEquivalents.computeIfAbsent(zscore, k -> getZScoreEquivalent(zscore));
        return score >= thresh;
    }

    private List<Double> recentScoresSorted;

    public void updateRecentScoreList(List<Double> recentScores) {
        recentScoresSorted = Lists.newArrayList(recentScores);
        Collections.sort(recentScoresSorted);
    }

    public boolean isPercentileOutlier(double score,
                                       double targetPercentile) {
        double thresh = recentScoresSorted.get((int)(targetPercentile*recentScoresSorted.size()));
        return score >= thresh;
    }

    private List<DatumWithScore> scoreBatch(List<Datum> data) {
        List<DatumWithScore> ret = new ArrayList<>();
        for(Datum d : data) {
            ret.add(new DatumWithScore(d, score(d)));
        }
        return ret;
    }

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
        int splitPoint = (int)(data.size()*percentile);
        List<DatumWithScore> scoredData = scoreBatch(data);

        scoredData.sort((a, b) -> a.getScore().compareTo(b.getScore()));
        sw.stop();
        long scoringTime = sw.elapsed(TimeUnit.MILLISECONDS);
        log.debug("...ended scoring (time: {}ms!", scoringTime);

        return new BatchResult(scoredData.subList(0, splitPoint),
                               scoredData.subList(splitPoint, scoredData.size()));

    }

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
        log.debug("...ended scoring (time: {}ms!", scoringTime);
        return new BatchResult(inliers, outliers);
    }
}
