package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 * Classify rows based on high / low values for a single column. Returns a new DataFrame with a
 * column representation the classification status for each row: 1.0 if outlier, 0.0 otherwise.
 */
public class PercentileClassifier extends Classifier implements ThresholdClassifier {

    // Parameters
    private double percentile = 0.5;
    private boolean includeHigh = true;
    private boolean includeLow = true;

    // Calculated values
    private double lowCutoff;
    private double highCutoff;
    private DataFrame output;

    public PercentileClassifier(String columnName) {
        super(columnName);
    }

    @Override
    public void process(DataFrame input) {
        double[] metrics = input.getDoubleColumnByName(columnName);
        int len = metrics.length;
        lowCutoff = new Percentile().evaluate(metrics, percentile);
        highCutoff = new Percentile().evaluate(metrics, 100.0 - percentile);

        output = input.copy();
        double[] resultColumn = new double[len];
        for (int i = 0; i < len; i++) {
            double curVal = metrics[i];
            if ((curVal > highCutoff && includeHigh)
                || (curVal < lowCutoff && includeLow)
                ) {
                resultColumn[i] = 1.0;
            }
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
    public PercentileClassifier setPercentile(double percentile) {
        this.percentile = percentile;
        return this;
    }

    public boolean isIncludeHigh() {
        return includeHigh;
    }

    /**
     * @param includeHigh Whether to count high points as outliers.
     * @return this
     */
    public PercentileClassifier setIncludeHigh(boolean includeHigh) {
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
    public PercentileClassifier setIncludeLow(boolean includeLow) {
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
