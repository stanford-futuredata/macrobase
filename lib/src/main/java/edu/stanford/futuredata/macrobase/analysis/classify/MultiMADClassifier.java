package edu.stanford.futuredata.macrobase.analysis.classify;

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
    private List<String> columnNames;
    private String outputColumnSuffix = "_OUTLIER";
    private boolean useParallel = false;
    private boolean useReservoirSampling = false;
    private final double trimmedMeanFallback = 0.05;
    // https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
    private final double MAD_TO_ZSCORE_COEFFICIENT = 1.4826;

    // Calculated values
    private DataFrame output;

    public MultiMADClassifier(String... columnNames) {
        this.columnNames = new ArrayList<String>(Arrays.asList(columnNames));
    }

    @Override
    public void process(DataFrame input) {
        output = input.copy();

        int sampleSize = (int)(input.getNumRows() * samplingRate);
        int[] sampleIndices = new int[sampleSize];
        if (samplingRate != 1) {
            sampleIndices = getRandomSample(input.getNumRows());
        }

        for (String columnName : columnNames) {
            double[] metrics = input.getDoubleColumnByName(columnName);

            double[] trainingMetrics = new double[sampleSize];
            if (samplingRate == 1) {
                System.arraycopy(metrics, 0, trainingMetrics, 0, metrics.length);
            } else {
                for (int j = 0; j < sampleIndices.length; j++) {
                    trainingMetrics[j] = metrics[sampleIndices[j]];
                }
            }

            Stats stats = train(trainingMetrics);
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

    private int[] getRandomSample(int numRows) {
        int sampleSize = (int)(numRows * samplingRate);
        int[] sampleIndices = new int[sampleSize];

        if (useReservoirSampling) {
            // Reservoir Sampling:
            for (int i = 0; i < sampleSize; i++) {
                sampleIndices[i] = i;
            }
            Random rand = new Random();
            for (int i = sampleSize; i < numRows; i++) {
                int j = rand.nextInt(i+1);
                if (j < sampleSize) {
                    sampleIndices[j] = i;
                }
            }
            boolean[] mask = new boolean[numRows];
            for (int i = 0; i < sampleSize; i++) {
                mask[sampleIndices[i]] = true;
            }
        } else {
            // partial Fisher-Yates shuffle
            int[] range = new int[numRows];
            for (int i = 0; i < numRows; i++) {
                range[i] = i;
            }
            Random rand = new Random();
            for (int i = 0; i < sampleSize; i++) {
                int j = rand.nextInt(numRows - i) + i;
                int temp = range[j];
                range[j] = range[i];
                range[i] = temp;
            }
            System.arraycopy(range, 0, sampleIndices, 0, sampleSize);
        }

        return sampleIndices;
    }

    private Stats train(double[] metrics) {
        double median = new Percentile().evaluate(metrics, 50);

        for (int i = 0; i < metrics.length; i++) {
            metrics[i] = Math.abs(metrics[i] - median);
        }

        double MAD = new Percentile().evaluate(metrics, 50);

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
        return this;
    }

    public MultiMADClassifier setColumnNames(String... columnNames) {
        this.columnNames = new ArrayList<String>(Arrays.asList(columnNames));
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

    private final class Stats {
        private final double median;
        private final double mad;

        public Stats(double median, double mad) {
            this.median = median;
            this.mad = mad;
        }
    }
}
