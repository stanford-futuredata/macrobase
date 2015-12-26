package macrobase.analysis.outlier;

import com.google.common.collect.Lists;
import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by pbailis on 12/14/15.
 */
public abstract class OutlierDetector {
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

    private Map<Double, Double> cachedPercentileEquivalents = new HashMap<>();

    public void clearScorePercentileCache() {
        cachedPercentileEquivalents.clear();
    }

    public boolean isPercentileOutlier(double score,
                                       double targetPercentile,
                                       List<Double> recentScores) {
        Double thresh = cachedPercentileEquivalents.get(score);
        if(thresh == null) {
            recentScores.sort((a, b) -> a.compareTo(b));
            thresh = recentScores.get((int)targetPercentile*recentScores.size());
            cachedPercentileEquivalents.put(score, thresh);
        }

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
        train(data);
        int splitPoint = (int)(data.size()-data.size()*percentile);
        List<DatumWithScore> scoredData = scoreBatch(data);

        scoredData.sort((a, b) -> a.getScore().compareTo(b.getScore()));

        return new BatchResult(scoredData.subList(0, splitPoint),
                               scoredData.subList(splitPoint, scoredData.size()));

    }

    public BatchResult classifyBatchByZScoreEquivalent(List<Datum> data,
                                                       double zscore) {

        List<DatumWithScore> inliers = new ArrayList<>();
        List<DatumWithScore> outliers = new ArrayList<>();

        train(data);

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
        return new BatchResult(inliers, outliers);
    }
}
