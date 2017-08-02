package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.operator.Transformer;

/** 
 * A classifier that uses a high or low threshold
 */
public interface ThresholdClassifier extends Transformer {
    public boolean isIncludeHigh();

    /**
     * @param includeHigh Whether to count high points as outliers.
     * @return this
     */
    public ThresholdClassifier setIncludeHigh(boolean includeHigh);
    public boolean isIncludeLow();

    /**
     * @param includeLow Whether to count low points as outliers
     * @return this
     */
    public ThresholdClassifier setIncludeLow(boolean includeLow);

    public double getLowCutoff();
    public double getHighCutoff();
}
