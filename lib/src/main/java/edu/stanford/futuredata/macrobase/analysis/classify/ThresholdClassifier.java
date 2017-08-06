package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.operator.Transformer;

/** 
 * A classifier that uses a high or low threshold
 */
public interface ThresholdClassifier extends Transformer {

    /**
     * @param includeHigh Whether to count high points as outliers.
     * @return this
     */
    ThresholdClassifier setIncludeHigh(boolean includeHigh);

    /**
     * @param includeLow Whether to count low points as outliers
     * @return this
     */
    ThresholdClassifier setIncludeLow(boolean includeLow);

    double getLowCutoff();
    double getHighCutoff();
}
