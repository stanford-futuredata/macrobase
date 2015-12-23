package macrobase.analysis.outlier;

import macrobase.analysis.summary.result.DatumWithScore;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.stat.descriptive.rank.Median;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class MAD extends OutlierDetector {
    private double median;
    private double MAD;

    // https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
    private final double MAD_TO_ZSCORE_COEFFICIENT = 1.4826;

    @Override
    public List<DatumWithScore> scoreData(List<Datum> data) {
        assert (data.get(0).getMetrics().getDimension() == 1);
        data.sort((x, y) -> Double.compare(x.getMetrics().getEntry(0),
                                           y.getMetrics().getEntry(0)));

        if (data.size() % 2 == 0) {
            median = (data.get(data.size() / 2 - 1).getMetrics().getEntry(0) +
                      data.get(data.size() / 2 + 1).getMetrics().getEntry(0)) / 2;
        } else {
            median = data.get((int) Math.ceil(data.size() / 2)).getMetrics().getEntry(0);
        }

        List<Double> residuals = new ArrayList<>(data.size());
        for (Datum d : data) {
            residuals.add(Math.abs(d.getMetrics().getEntry(0) - median));
        }

        residuals.sort((a, b) -> Double.compare(a, b));

        if (data.size() % 2 == 0) {
            MAD = (residuals.get(data.size() / 2 - 1) +
                   residuals.get(data.size() / 2 + 1)) / 2;
        } else {
            MAD = residuals.get((int) Math.ceil(data.size() / 2));
        }

        List<DatumWithScore> ret = new ArrayList<>();

        for (Datum d : data) {
            double point = d.getMetrics().getEntry(0);
            double score = Math.abs(point - median) / (MAD);

            ret.add(new DatumWithScore(d, score));
        }

        return ret;
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        return zscore*MAD_TO_ZSCORE_COEFFICIENT;
    }
}
