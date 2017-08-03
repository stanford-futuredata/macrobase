package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;

public interface Pipeline {
    Explanation results() throws Exception;
}
