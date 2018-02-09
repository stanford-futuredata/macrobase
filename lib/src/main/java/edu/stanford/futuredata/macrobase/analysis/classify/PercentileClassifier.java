package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import java.util.BitSet;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 * Classify rows based on high / low values for a single column. Returns a new dataframe with a
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

    /**
     * Alternate constructor that takes in List of Strings; used to instantiate Classifier (via
     * reflection) specified in MacroBase SQL query
     *
     * @param attrs by convention, should be a List that has 2-4 values: [outlier_col_name, cutoff,
     * includeHigh (optional), includeLo (optional)]
     */
    public PercentileClassifier(List<String> attrs) throws MacrobaseException {
        this(attrs.get(0));
        percentile = 100 * (1 - Double
            .parseDouble(attrs.get(1))); // TODO: this is stupid -- we need to standardize this
        includeHigh =
            (attrs.size() <= 2) || Boolean
                .parseBoolean(attrs.get(2)); // 3rd arg if present else true
        includeLow =
            (attrs.size() > 3) && Boolean
                .parseBoolean(attrs.get(3)); // 4th arg if present else false
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
    public BitSet getMask(DataFrame input) {
        final double[] inputCol = input.getDoubleColumnByName(columnName);
        final int numRows = inputCol.length;
        lowCutoff = new Percentile().evaluate(inputCol, percentile);
        highCutoff = new Percentile().evaluate(inputCol, 100.0 - percentile);
        final BitSet mask = new BitSet(numRows);

        for (int i = 0; i < numRows; i++) {
            double curVal = inputCol[i];
            if ((curVal > highCutoff && includeHigh)
                || (curVal < lowCutoff && includeLow)
                ) {
                mask.set(i);
            }
        }
        return mask;
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
