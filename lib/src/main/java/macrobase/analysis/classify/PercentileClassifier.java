package macrobase.analysis.classify;

import macrobase.datamodel.DataFrame;
import macrobase.operator.Transformer;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 * Classify rows based on high / low values for a single column.
 * Returns a new dataframe with a column representation the classification status for
 * each row: 1.0 if outlier, 0.0 otherwise.
 */
public class PercentileClassifier implements Transformer {
    // Parameters
    private double percentile = 0.5;
    private boolean includeHigh = true;
    private boolean includeLow = true;
    private String columnName;
    private String outputColumnName = "_OUTLIER";

    // Calculated values
    private double lowCutoff;
    private double highCutoff;
    private DataFrame output;

    public PercentileClassifier(String columnName) {
        this.columnName = columnName;
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
    public String getColumnName() {
        return columnName;
    }
    public PercentileClassifier setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }
    public String getOutputColumnName() {
        return outputColumnName;
    }

    /**
     * @param outputColumnName Which column to write the classification results.
     * @return this
     */
    public PercentileClassifier setOutputColumnName(String outputColumnName) {
        this.outputColumnName = outputColumnName;
        return this;
    }

    public double getLowCutoff() {
        return lowCutoff;
    }
    public double getHighCutoff() {
        return highCutoff;
    }
}
