package edu.stanford.futuredata.macrobase.analysis.summary.apriori;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

public class ExplanationResultGrouper {
    private Explanation explanation;
    private DataFrame data;

    public ExplanationResultGrouper(
            Explanation e,
            DataFrame d
    ) {
        this.explanation = e;
        this.data = d;
    }

    public void group() {

    }
}
