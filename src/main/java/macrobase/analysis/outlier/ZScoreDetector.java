package macrobase.analysis.outlier;

import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

public class ZScoreDetector implements OutlierDetector {
    private double mean;
    private double std;
    private final double zscoreThresh;

    public ZScoreDetector(Double _zscore) {
        zscoreThresh = _zscore;
    }

    @Override
    public BatchResult classifyBatch(List<Datum> data) {
        double sum = 0;

        for(Datum d : data) {
            assert(d.getMetrics().getDimension() == 1);
            sum += d.getMetrics().getEntry(0);
        }
        mean = sum/data.size();

        double ss = 0;
        for(Datum d : data) {
            ss += Math.pow(mean - d.getMetrics().getEntry(0), 2);
        }
        std = Math.sqrt(ss / data.size());

        List<DatumWithScore> inliers = new ArrayList<>();
        List<DatumWithScore> outliers = new ArrayList<>();

        for(Datum d : data) {
            double point = d.getMetrics().getEntry(0);
            double score = Math.abs(point-mean)/std;

            if(score > zscoreThresh) {
                outliers.add(new DatumWithScore(d, score));
            } else {
                inliers.add(new DatumWithScore(d, score));
            }
        }

        return new OutlierDetector.BatchResult(inliers, outliers);
    }
}
