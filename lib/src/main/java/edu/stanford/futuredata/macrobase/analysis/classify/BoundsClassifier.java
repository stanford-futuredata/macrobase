package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.classify.stats.WeightedPercentile;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

/**
 * Classify rows by high / low values based on bounds on the possible number of outliers.
 * Bounds are constructed using the mean, standard deviation, minimum and maximum of each row.
 * Returns a new dataframe with a column representation of the estimated number of outliers
 * for each group, which can be non-integer.
 */
public class BoundsClassifier extends CubeClassifier implements ThresholdClassifier {
    // Parameters
    private String meanColumnName = "mean";
    private String stdColumnName = "std";
    private String minColumnName = "min";
    private String maxColumnName = "max";
    private double percentile = 1.0;
    private boolean includeHigh = true;
    private boolean includeLow = true;

    // Calculated values
    private double lowCutoff;
    private double highCutoff;
    private DataFrame output;

    public BoundsClassifier(String countColumnName, String meanColumnName,
                            String stdColumnName, String minColumnName, String maxColumnName) {
        super(countColumnName);
        this.meanColumnName = meanColumnName;
        this.stdColumnName = stdColumnName;
        this.minColumnName = minColumnName;
        this.maxColumnName = maxColumnName;
    }

    @Override
    public void process(DataFrame input) {
        double[] means = input.getDoubleColumnByName(meanColumnName);
        double[] counts = input.getDoubleColumnByName(countColumnName);
        double[] stds = input.getDoubleColumnByName(stdColumnName);
        double[] mins = input.getDoubleColumnByName(minColumnName);
        double[] maxs = input.getDoubleColumnByName(maxColumnName);
        int len = means.length;

        WeightedPercentile wp = new WeightedPercentile(counts, means);
        lowCutoff = wp.evaluate(percentile);
        highCutoff = wp.evaluate(100.0 - percentile);

        output = input.copy();
        double[] resultColumn = new double[len];
        for (int i = 0; i < len; i++) {
            double mean = means[i];
            double std = stds[i];
            double count = counts[i];
            double min = mins[i];
            double max = maxs[i];
            double numOutliers = 0.0;
            if (Double.isNaN(std) || std == 0.0) {
                // only one metric in group, or all metrics are equal
                if ((includeHigh && mean > highCutoff)
                        || (includeLow && mean < highCutoff)) {
                    numOutliers += count;
                }
            } else {
                // We use lower bounds as the heuristic for the number of outliers.
                // The maxStdBound is based on the fact that the true standard devation must be
                // no greater than the maximum possible standard deviation. It is computed using
                // an inequality that extends Chebyshev's inequality to take advantage of knowing
                // the minimum and maximum.
                // The minStdBound is based on the fact that the true standard deviation must be
                // no less than the minimum possible standard deviation. It is computed using
                // Cantelli's inequality.
                if (includeHigh) {
                    if (highCutoff >= max) {
                        numOutliers += 0.0;
                    } else if (highCutoff < min) {
                        numOutliers += count;
                    } else {
                        double maxStdBound = (((std * std * (count - 1) / count) + (min - mean) * (highCutoff - mean)) /
                                ((max - highCutoff) * (max - min)));
                        double minStdBound;
                        double markovBound = 1 - (max - mean) / (max - highCutoff);;
                        if (mean <= highCutoff) {
                            minStdBound = 0;
                        } else {
                            minStdBound = ((highCutoff - mean) * (highCutoff - mean)) /
                                    (((highCutoff - mean) * (highCutoff - mean)) + (std * std * (count - 1) / count));
                        }
                        numOutliers += count * Math.max(maxStdBound, Math.max(minStdBound, markovBound));
                    }
                }
                if (includeLow) {
                    if (lowCutoff <= min) {
                        numOutliers += 0.0;
                    } else if (lowCutoff > max) {
                        numOutliers += count;
                    } else {
                        double maxStdBound = (((std * std * (count - 1) / count) + (max - mean) * (lowCutoff - mean)) /
                                ((min - lowCutoff) * (min - max)));
                        double minStdBound;
                        double markovBound = 1 - (mean - min) / (lowCutoff - min);;
                        if (mean >= lowCutoff) {
                            minStdBound = 0;
                        } else {
                            minStdBound = ((mean - lowCutoff) * (mean - lowCutoff)) /
                                    (((mean - lowCutoff) * (mean - lowCutoff)) + (std * std * (count - 1) / count));
                        }
                        numOutliers += count * Math.max(maxStdBound, Math.max(minStdBound, markovBound));
                    }
                }
            }
            resultColumn[i] = numOutliers;
        }
        output.addDoubleColumn(outputColumnName, resultColumn);
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    // Parameter Getters and Setters
    public double getPercentile() {
        return percentile;
    }

    /**
     * @param percentile Cutoff point for high or low values
     * @return this
     */
    public BoundsClassifier setPercentile(double percentile) {
        this.percentile = percentile;
        return this;
    }

    public String getMeanColumnName() {
        return meanColumnName;
    }

    /**
     * @param meanColumnName Which column contains the mean of each row's attribute
     *                       combination.
     * @return this
     */
    public BoundsClassifier setMeanColumnName(String meanColumnName) {
        this.meanColumnName = meanColumnName;
        return this;
    }

    public String getStdColumnName() {
        return stdColumnName;
    }

    /**
     * @param stdColumnName Which column contains the standard deviation of metrics for events
     *                      corresponding to each row's attribute combination. Assumed to contain
     *                      the sample standard deviation.
     * @return this
     */
    public BoundsClassifier setStdColumnName(String stdColumnName) {
        this.stdColumnName = stdColumnName;
        return this;
    }

    public String getMinColumnName() {
        return minColumnName;
    }

    /**
     * @param minColumnName Which column contains the minimum of metrics for events
     *                      corresponding to each row's attribute combination.
     * @return this
     */
    public BoundsClassifier setMinColumnName(String minColumnName) {
        this.minColumnName = minColumnName;
        return this;
    }

    public String getMaxColumnName() {
        return maxColumnName;
    }

    /**
     * @param maxColumnName Which column contains the maximum of metrics for events
     *                      corresponding to each row's attribute combination.
     * @return this
     */
    public BoundsClassifier setMaxColumnName(String maxColumnName) {
        this.maxColumnName = maxColumnName;
        return this;
    }

    public boolean isIncludeHigh() {
        return includeHigh;
    }

    /**
     * @param includeHigh Whether to count high points as outliers.
     * @return this
     */
    public BoundsClassifier setIncludeHigh(boolean includeHigh) {
        this.includeHigh = includeHigh;
        return this;
    }
    public boolean isIncludeLow() {
        return includeLow;
    }

    /**
     * @param includeLow Whether to count low points as outliers
     * @return this
     */
    public BoundsClassifier setIncludeLow(boolean includeLow) {
        this.includeLow = includeLow;
        return this;
    }

    public double getLowCutoff() {
        return lowCutoff;
    }
    public double getHighCutoff() {
        return highCutoff;
    }
}
