package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.sample.*;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.operator.Transformer;

import org.apache.commons.math3.special.Erf;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.lang.Math;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * This class processes a batch of rows. For each input metric column, it classifies the rows as
 * outliers or inliers based on the median absolute deviation (MAD) from the median, and writes
 * the classification results to a new output column. Each input column is considered as an
 * independent unit. For example, if there are five input metric columns, each row is classified
 * five times, once for each column, and five new ouput columns will be created.
 */
public class MultiMADClassifier implements Transformer {
    private double percentile = 0.5;
    private double zscore = 2.576;
    private boolean includeHigh = true;
    private boolean includeLow = true;
    private double samplingRate = 1;
    private Sampler sampler = null;
    private boolean useReservoirSampling = false;
    private List<String> columnNames;
    private String outputColumnSuffix = "_OUTLIER";
    private boolean useParallel = false;
    
    private final double trimmedMeanFallback = 0.05;
    // https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
    private final double MAD_TO_ZSCORE_COEFFICIENT = 1.4826;

    // Calculated values
    private DataFrame output;

    public MultiMADClassifier(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public MultiMADClassifier(String... columnNames) {
        this.columnNames = new ArrayList<String>(Arrays.asList(columnNames));
    }

    @Override
    public void process(DataFrame input) {
        output = input.copy();

        if (samplingRate != 1) {
            sampler.computeSampleIndices(input.getNumRows(), samplingRate);
        }

        for (String columnName : columnNames) {
            double[] metrics = input.getDoubleColumnByName(columnName);

            Stats stats = train(metrics);
            double lowCutoff = stats.median - (zscore * stats.mad);
            double highCutoff = stats.median + (zscore * stats.mad);

            if (useParallel) {
                // Parallelized stream scoring:
                double[] results = new double[metrics.length];
                System.arraycopy(metrics, 0, results, 0, metrics.length);
                Arrays.parallelSetAll(results, m -> (
                    (m > highCutoff && includeHigh) || (m < lowCutoff && includeLow)) ? 1.0 : 0.0);
                output.addDoubleColumn(columnName + outputColumnSuffix, results);
            } else {
                // Non-parallel scoring:
                double[] results = new double[metrics.length];
                for (int r = 0; r < input.getNumRows(); r++) {
                    boolean isOutlier = (metrics[r] > highCutoff && includeHigh) ||
                        (metrics[r] < lowCutoff && includeLow);
                    results[r] = isOutlier ? 1.0 : 0.0;
                }
                output.addDoubleColumn(columnName + outputColumnSuffix, results);
            }
        }
    }

    private Stats train(double[] metrics) {
        Percentile percentile = new Percentile();

        double[] trainingMetrics;  // used to compute the median
        if (samplingRate == 1) {
            trainingMetrics = metrics;
        } else {
            trainingMetrics = sampler.getSample(metrics);
        }

        double median = percentile.evaluate(trainingMetrics, 50);

        double[] residuals;  // used to compute the MAD
        if (samplingRate == 1) {
            residuals = new double[trainingMetrics.length];
        } else {
            residuals = trainingMetrics;
        }
        for (int i = 0; i < trainingMetrics.length; i++) {
            residuals[i] = Math.abs(trainingMetrics[i] - median);
        }

        double MAD = percentile.evaluate(residuals, 50);

        // If MAD is 0, we use the average of residuals
        if (MAD == 0) {
            Arrays.sort(residuals);
            int lowerTrimmedMeanIndex = (int) (residuals.length * trimmedMeanFallback);
            int upperTrimmedMeanIndex = (int) (residuals.length * (1 - trimmedMeanFallback));
            double sum = 0;
            for (int i = residuals.length/2; i < upperTrimmedMeanIndex; ++i) {
                sum += metrics[i];
            }
            MAD = sum / (upperTrimmedMeanIndex - lowerTrimmedMeanIndex);
            assert (MAD != 0);
        }

        Stats stats = new Stats(median, MAD * MAD_TO_ZSCORE_COEFFICIENT);
        return stats;
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    // Parameter Getters and Setters
    public double getPercentile() {
        return percentile;
    }

    public double getZscore() {
        return zscore;
    }

    public double getSamplingRate() {
        return samplingRate;
    }

    public boolean isIncludeHigh() {
        return includeHigh;
    }

    public boolean isIncludeLow() {
        return includeLow;
    }

    public boolean isUseParallel() {
        return useParallel;
    }

    public boolean isUseReservoirSampling() {
        return useReservoirSampling;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public String getOutputColumnSuffix() {
        return outputColumnSuffix;
    }

    /**
     * @param percentile Approximate percent of data to classify as outlier on each end of
     * spectrum (i.e., a percentile of 5 means that the top 5% and bottom 5% are outliers)
     * @return this
     */
    public MultiMADClassifier setPercentile(double percentile) {
        this.percentile = percentile;
        // https://en.wikipedia.org/wiki/Normal_distribution#Quantile_function
        this.zscore = Math.sqrt(2) * Erf.erfcInv(2.0*percentile/100.0);
        return this;
    }

    /**
     * @param zscore Z-score above/below which data is classified as outlier
     * @return this
     */
    public MultiMADClassifier setZscore(double zscore) {
        this.zscore = zscore;
        return this;
    }

    /**
     * @param samplingRate Rate to sample elements used to calculate median and MAD, must be
     * greater than 0 and at most 1 (i.e., use all elements without sampling)
     * @return this
     */
    public MultiMADClassifier setSamplingRate(double samplingRate) {
        this.samplingRate = samplingRate;
        if (samplingRate != 1 && this.sampler == null) {
            if (useReservoirSampling) {
                this.sampler = new ReservoirSampler();
            } else {
                this.sampler = new FisherYatesSampler();
            }
        }
        return this;
    }

    /**
     * @param includeHigh Whether to count high points as outliers.
     * @return this
     */
    public MultiMADClassifier setIncludeHigh(boolean includeHigh) {
        this.includeHigh = includeHigh;
        return this;
    }
    
    /**
     * @param includeLow Whether to count low points as outliers
     * @return this
     */
    public MultiMADClassifier setIncludeLow(boolean includeLow) {
        this.includeLow = includeLow;
        return this;
    }

    /**
     * @param useParallel Whether to use parallelization, can be beneficial for large datasets
     * @return this
     */
    public MultiMADClassifier setUseParallel(boolean useParallel) {
        this.useParallel = useParallel;
        return this;
    }

    /**
     * @param useReservoirSampling Whether to sample using reservoir sampling. Otherwise,
     * a partial Fisher-Yates shuffle is used. Regardless of the sampling rate, teservoir sampling
     * will take the same amount of time. However, a partial Fisher-Yates shuffle will be
     * faster for smaller sampling rates. Empirically, the Fisher-Yates shuffle works better than
     * reservoir sampling when samplingRate is less than 0.3.
     * @return this
     */
    public MultiMADClassifier setUseReservoirSampling(boolean useReservoirSampling) {
        this.useReservoirSampling = useReservoirSampling;
        if (sampler != null) {
            if (useReservoirSampling && !sampler.getSamplingMethod().equals("reservoir")) {
                sampler = new ReservoirSampler();
            } else if (!useReservoirSampling && sampler.getSamplingMethod().equals("reservoir")) {
                sampler = new FisherYatesSampler();
            }
        }
        return this;
    }

    public MultiMADClassifier setColumnNames(String... columnNames) {
        this.columnNames = new ArrayList<String>(Arrays.asList(columnNames));
        return this;
    }

    public MultiMADClassifier setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
        return this;
    }

    /**
     * @param outputColumnSuffix The output for column "a" would be in a column named
     * "a" + outputColumnSuffix.
     * @return this
     */
    public MultiMADClassifier setOutputColumnSuffix(String outputColumnSuffix) {
        this.outputColumnSuffix = outputColumnSuffix;
        return this;
    }

    public final class Stats {
        private final double median;
        private final double mad;

        public Stats(double median, double mad) {
            this.median = median;
            this.mad = mad;
        }
    }
}
