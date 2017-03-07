package macrobase.analysis.summary;

import macrobase.datamodel.DataFrame;
import macrobase.operator.Operator;

import java.util.List;

public class SimpleOutlierSummarizer implements Operator<DataFrame, List<OutlierGroup>> {
    // Parameters
    public int minOutlierCount = 5;
    public int outlierColumn = 0;

    // Output
    private List<OutlierGroup> results;

    public SimpleOutlierSummarizer() {}

    @Override
    public Operator process(DataFrame df) {
        return null;
    }

    @Override
    public List<OutlierGroup> getResults() {
        return null;
    }
}
