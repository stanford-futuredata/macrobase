package edu.stanford.futuredata.macrobase.analysis.stats;

import java.util.Arrays;
import java.util.List;

public class MAD {
    private double median;
    private double MAD;

    private final double trimmedMeanFallback = 0.05;

    // https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
    private final double MAD_TO_ZSCORE_COEFFICIENT = 1.4826;
    
    public MAD() {}

    public void train(double[] metrics) {
        int len = metrics.length;

        Arrays.sort(metrics);

        if (len % 2 == 0) {
            median = (metrics[len / 2 - 1] + metrics[len / 2]) / 2;
        } else {
            median = metrics[(int) Math.ceil(len / 2)];
        }

        double[] residuals = new double[len];
        for (int i = 0; i < len; i++) {
            residuals[i] = Math.abs(metrics[i] - median);
        }

        Arrays.sort(residuals);

        if (len % 2 == 0) {
            MAD = (residuals[len / 2 - 1] +
                   residuals[len / 2]) / 2;
        } else {
            MAD = residuals[(int) Math.ceil(len / 2)];
        }

        if (MAD == 0) {
            int lowerTrimmedMeanIndex = (int) (residuals.length * trimmedMeanFallback);
            int upperTrimmedMeanIndex = (int) (residuals.length * (1 - trimmedMeanFallback));
            double sum = 0;
            for (int i = lowerTrimmedMeanIndex; i < upperTrimmedMeanIndex; ++i) {
                sum += residuals[i];
            }
            MAD = sum / (upperTrimmedMeanIndex - lowerTrimmedMeanIndex);
            assert (MAD != 0);
        }
    }

    public double score(double point) {
        return Math.abs(point - median) / (MAD);
    }

    public double getZScoreEquivalent(double zscore) {
        return zscore / MAD_TO_ZSCORE_COEFFICIENT;
    }

    public double getMedian() {
        return median;
    }

    public double getMAD() {
        return MAD;
    }
}