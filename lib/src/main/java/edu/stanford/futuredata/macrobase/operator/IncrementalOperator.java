package edu.stanford.futuredata.macrobase.operator;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

public interface IncrementalOperator<O> extends Operator<DataFrame,  O> {
    void setWindowSize(int numPanes);
    int getWindowSize();
}
