package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

public class RawClassifier extends CubeClassifier {
    protected DataFrame input;

    @Override
    public boolean isIncludeHigh() {
        return false;
    }

    @Override
    public ThresholdClassifier setIncludeHigh(boolean includeHigh) {
        return null;
    }

    @Override
    public boolean isIncludeLow() {
        return false;
    }

    @Override
    public ThresholdClassifier setIncludeLow(boolean includeLow) {
        return null;
    }

    @Override
    public double getLowCutoff() {
        return 0;
    }

    @Override
    public double getHighCutoff() {
        return 0;
    }

    public RawClassifier(
            String totalCountColumn,
            String outlierCountColumn
    ){
        super(totalCountColumn);
        this.outputColumnName = outlierCountColumn;
    }

    @Override
    public void process(DataFrame input) throws Exception {
        this.input = input;
    }

    @Override
    public DataFrame getResults() {
        return input;
    }
}
