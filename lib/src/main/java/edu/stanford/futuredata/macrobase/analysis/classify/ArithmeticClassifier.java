package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.analysis.classify.stats.NormalDist;
import edu.stanford.futuredata.macrobase.analysis.classify.stats.WeightedPercentile;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.lang.Double;

/**
 * Classify rows by high / low values based on the group mean and standard deviation.
 * Returns a new dataframe with a column representation of the estimated number of outliers
 * for each group, which can be non-integer.
 */
public class ArithmeticClassifier extends CubeClassifier implements ThresholdClassifier {
    // Parameters
    private String meanColumnName = "mean";
    private String stdColumnName = "std";
    private double percentile = 1.0;
    private boolean includeHigh = true;
    private boolean includeLow = true;

    // Calculated values
    private double lowCutoff;
    private double highCutoff;
    private DataFrame output;

    public ArithmeticClassifier(String countColumnName, String meanColumnName,
                                String stdColumnName) {
        super(countColumnName);
        this.meanColumnName = meanColumnName;
        this.stdColumnName = stdColumnName;
    }

    @Override
    public void process(DataFrame input) {
        double[] means = input.getDoubleColumnByName(meanColumnName);
        double[] counts = input.getDoubleColumnByName(countColumnName);
        double[] stds = input.getDoubleColumnByName(stdColumnName);
        int len = means.length;

        WeightedPercentile wp = new WeightedPercentile(counts, means);
        lowCutoff = wp.evaluate(percentile);
        highCutoff = wp.evaluate(100.0 - percentile);

        output = input.copy();
        double[] resultColumn = new double[len];
        NormalDist dist = new NormalDist();
        for (int i = 0; i < len; i++) {
            double mean = means[i];
            double std = stds[i];
            double count = counts[i];
            double numOutliers = 0.0;
            if (Double.isNaN(std) || std == 0.0) {
                // only one metric in group, or all metrics are equal
                if ((includeHigh && mean > highCutoff)
                        || (includeLow && mean < highCutoff)) {
                    numOutliers = count;
                }
            } else {
                if (includeHigh) {
                    double percentile = dist.cdf(mean, std, highCutoff);
                    numOutliers += count * (1.0 - percentile);
                }
                if (includeLow) {
                    double percentile = dist.cdf(mean, std, lowCutoff);
                    numOutliers += count * percentile;
                }
            }
            resultColumn[i] = numOutliers;
        }
        output.addColumn(outputColumnName, resultColumn);
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
    public ArithmeticClassifier setPercentile(double percentile) {
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
    public ArithmeticClassifier setMeanColumnName(String meanColumnName) {
        this.meanColumnName = meanColumnName;
        return this;
    }

    public String getStdColumnName() {
        return stdColumnName;
    }

    /**
     * @param stdColumnName Which column contains the standard deviation of metrics for events
     *                      corresponding to each row's attribute combination.
     * @return this
     */
    public ArithmeticClassifier setStdColumnName(String stdColumnName) {
        this.stdColumnName = stdColumnName;
        return this;
    }

    public boolean isIncludeHigh() {
        return includeHigh;
    }

    /**
     * @param includeHigh Whether to count high points as outliers.
     * @return this
     */
    public ArithmeticClassifier setIncludeHigh(boolean includeHigh) {
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
    public ArithmeticClassifier setIncludeLow(boolean includeLow) {
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
