package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

public interface Pipeline {
    Explanation results() throws Exception;
}
