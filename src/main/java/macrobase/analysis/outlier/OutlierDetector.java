package macrobase.analysis.outlier;

import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

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

    public abstract List<DatumWithScore>scoreData(List<Datum> data);
    public abstract double getZScoreEquivalent(double zscore);

    public BatchResult classifyBatchByPercentile(List<Datum> data,
                                                 double percentile) {
        int splitPoint = (int)(data.size()-data.size()*percentile);
        List<DatumWithScore> scoredData = scoreData(data);

        scoredData.sort((a, b) -> a.getScore().compareTo(b.getScore()));

        return new BatchResult(scoredData.subList(0, splitPoint),
                               scoredData.subList(splitPoint, scoredData.size()));

    }

    public BatchResult classifyBatchByZScoreEquivalent(List<Datum> data,
                                                       double zscore) {

        List<DatumWithScore> inliers = new ArrayList<>();
        List<DatumWithScore> outliers = new ArrayList<>();

        double threshScore = getZScoreEquivalent(zscore);

        // take an efficiency hit for modularity :(
        List<DatumWithScore> scoredData = scoreData(data);
        for (DatumWithScore d : scoredData) {
            if (d.getScore() > threshScore) {
                outliers.add(d);
            } else {
                inliers.add(d);
            }
        }
        return new BatchResult(inliers, outliers);
    }
}
