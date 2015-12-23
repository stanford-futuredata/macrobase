package macrobase.analysis.outlier;

import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

public class ZScore extends OutlierDetector {
    private double mean;
    private double std;

    @Override
    public List<DatumWithScore> scoreData(List<Datum> data) {
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

        List<DatumWithScore> ret = new ArrayList<>();

        for(Datum d : data) {
            double point = d.getMetrics().getEntry(0);
            double score = Math.abs(point-mean)/std;

            ret.add(new DatumWithScore(d, score));
        }

        return ret;
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        // z-score is identity since we're literally calculating the z-score
        return zscore;
    }
}
