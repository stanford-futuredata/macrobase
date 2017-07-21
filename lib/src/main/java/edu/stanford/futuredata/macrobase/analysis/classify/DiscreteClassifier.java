package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 * Classify rows by high / low values based on the group mean.
 * Returns a new dataframe with a column representation of the estimated number of outliers
 * for each group, which will either be zero or all elements in the group.
 */
public class DiscreteClassifier extends CubeClassifier implements ThresholdClassifier {
    // Parameters
    private String meanColumnName = "mean";
    private double percentile = 0.5;
    private boolean includeHigh = true;
    private boolean includeLow = true;

    // Calculated values
    private double lowCutoff;
    private double highCutoff;
    private DataFrame output;

    public DiscreteClassifier(String meanColumnName, String countColumnName) {
        super(countColumnName);
        this.meanColumnName = meanColumnName;
    }

    @Override
    public void process(DataFrame input) {
        double[] means = input.getDoubleColumnByName(meanColumnName);
        double[] counts = input.getDoubleColumnByName(countColumnName);
        int len = means.length;
        int numRawMetrics = 0;
        for (int i = 0; i < len; i++) {
            numRawMetrics += counts[i];
        }
        double[] rawMetrics = new double[numRawMetrics];
        int cumRawMetrics = 0;
        for (int i = 0; i < len; i++) {
            for (int j = cumRawMetrics; j < cumRawMetrics + counts[i]; j++) {
                rawMetrics[j] = means[i];
            }
            cumRawMetrics += counts[i];
        }
        lowCutoff = new Percentile().evaluate(rawMetrics, percentile);
        highCutoff = new Percentile().evaluate(rawMetrics, 100.0 - percentile);

        output = input.copy();
        double[] resultColumn = new double[len];
        for (int i = 0; i < len; i++) {
            double mean = means[i];
            if ((mean > highCutoff && includeHigh)
                    || (mean < lowCutoff && includeLow)
                    ) {
                resultColumn[i] = 1.0;
            }
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
    public DiscreteClassifier setPercentile(double percentile) {
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
    public DiscreteClassifier setMeanColumnName(String meanColumnName) {
        this.meanColumnName = meanColumnName;
        return this;
    }

    public boolean isIncludeHigh() {
        return includeHigh;
    }

    /**
     * @param includeHigh Whether to count high points as outliers.
     * @return this
     */
    public DiscreteClassifier setIncludeHigh(boolean includeHigh) {
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
    public DiscreteClassifier setIncludeLow(boolean includeLow) {
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
