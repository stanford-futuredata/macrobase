package macrobase.analysis.outlier;

import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

public class ZScore extends OutlierDetector {
    private double mean;
    private double std;

    @Override
    public void train(List<Datum> data) {
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
    }

    @Override
    public double score(Datum datum) {
        double point = datum.getMetrics().getEntry(0);
        return Math.abs(point-mean)/std;
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        // z-score is identity since we're literally calculating the z-score
        return zscore;
    }
}
