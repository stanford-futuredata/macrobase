package edu.stanford.futuredata.macrobase.analysis.classify;

/** 
 * A classifier that uses a high or low threshold
 */
public abstract class ThresholdClassifier extends Classifier{
    // Parameters
    protected boolean includeHigh = true;
    protected boolean includeLow = true;

    // Calculated values
    protected double lowCutoff;
    protected double highCutoff;

    public ThresholdClassifier(String columnName) {
        super(columnName);
    }

    public boolean isIncludeHigh() {
        return includeHigh;
    }

    /**
     * @param includeHigh Whether to count high points as outliers.
     * @return this
     */
    public ThresholdClassifier setIncludeHigh(boolean includeHigh) {
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
    public ThresholdClassifier setIncludeLow(boolean includeLow) {
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
