package macrobase.analysis.classify;

import macrobase.datamodel.DataFrame;
import macrobase.operator.Transformer;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

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
    public PercentileClassifier setPercentile(double percentile) {
        this.percentile = percentile;
        return this;
    }
    public boolean isIncludeHigh() {
        return includeHigh;
    }
    public PercentileClassifier setIncludeHigh(boolean includeHigh) {
        this.includeHigh = includeHigh;
        return this;
    }
    public boolean isIncludeLow() {
        return includeLow;
    }
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
