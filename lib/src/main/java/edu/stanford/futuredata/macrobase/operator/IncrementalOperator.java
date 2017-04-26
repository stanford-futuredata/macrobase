package edu.stanford.futuredata.macrobase.operator;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

/**
 * An operator which supports partial updates in the form of "minibatches" of data.
 * Results will be computed over the n previous minibatches, which is determined by setWindowSize,
 * so this operator can be used to implement streaming algorithms.
 *
 * Wrap this with WindowedOperator to support time-aware sliding window operators rather than work
 * directly with a fixed number of minibatches.
 * @param <O> Output type of operator
 */
public interface IncrementalOperator<O> extends Operator<DataFrame,  O> {
    void setWindowSize(int numPanes);
    int getWindowSize();
}
