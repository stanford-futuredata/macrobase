package edu.stanford.futuredata.macrobase.analysis.stats;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;

public class MAD {
    public double median;
    public double MAD;

    private final double trimmedMeanFallback = 0.05;

    // https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
    public final double MAD_TO_ZSCORE_COEFFICIENT = 1.4826;
    
    public MAD() {}

    public void train_old(double[] metrics) {
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

    // Note: metrics is modified
    public void train(double[] metrics) {
        median = new Percentile().evaluate(metrics, 50);

        for (int i = 0; i < metrics.length; i++) {
            metrics[i] = Math.abs(metrics[i] - median);
        }

        MAD = new Percentile().evaluate(metrics, 50);

        if (MAD == 0) {
            Arrays.sort(metrics);
            int lowerTrimmedMeanIndex = (int) (metrics.length * trimmedMeanFallback);
            int upperTrimmedMeanIndex = (int) (metrics.length * (1 - trimmedMeanFallback));
            double sum = 0;
            for (int i = lowerTrimmedMeanIndex; i < upperTrimmedMeanIndex; ++i) {
                sum += metrics[i];
            }
            MAD = sum / (upperTrimmedMeanIndex - lowerTrimmedMeanIndex);
            assert (MAD != 0);
        }
    }
    
    // public void train(double[] metrics) {
    //     median = new Percentile().evaluate(metrics, 50);

    //     double[] residuals = new double[metrics.length];
    //     for (int i = 0; i < metrics.length; i++) {
    //         residuals[i] = Math.abs(metrics[i] - median);
    //     }

    //     MAD = new Percentile().evaluate(residuals, 50);

    //     if (MAD == 0) {
    //         Arrays.sort(residuals);
    //         int lowerTrimmedMeanIndex = (int) (residuals.length * trimmedMeanFallback);
    //         int upperTrimmedMeanIndex = (int) (residuals.length * (1 - trimmedMeanFallback));
    //         double sum = 0;
    //         for (int i = lowerTrimmedMeanIndex; i < upperTrimmedMeanIndex; ++i) {
    //             sum += residuals[i];
    //         }
    //         MAD = sum / (upperTrimmedMeanIndex - lowerTrimmedMeanIndex);
    //         assert (MAD != 0);
    //     }
    // }

    public void train_iqr(double[] metrics) {
        Percentile percentile = new Percentile();
        percentile.setData(metrics);
        median = percentile.evaluate(50);
        // MAD is set as half of IQR
        MAD = (percentile.evaluate(75) - percentile.evaluate(25)) / 2;

        if (MAD == 0) {
            double[] residuals = new double[metrics.length];
            for (int i = 0; i < metrics.length; i++) {
                residuals[i] = Math.abs(metrics[i] - median);
            }

            Arrays.sort(residuals);
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

    public void train_iqr_par(double[] metrics) {
        int len = metrics.length;

        Arrays.parallelSort(metrics);
        if (len % 2 == 0) {
            median = (metrics[len / 2 - 1] + metrics[len / 2]) / 2;
        } else {
            median = metrics[(int) Math.ceil(len / 2)];
        }

        // MAD is set as half of IQR

        MAD = (metrics[3*len/4] - metrics[len/4]) / 2;

        if (MAD == 0) {
            double[] residuals = new double[metrics.length];
            for (int i = 0; i < metrics.length; i++) {
                residuals[i] = Math.abs(metrics[i] - median);
            }

            Arrays.sort(residuals);
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

    public double zscore(double point) {
        return Math.abs(point - median) / (MAD * MAD_TO_ZSCORE_COEFFICIENT);
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