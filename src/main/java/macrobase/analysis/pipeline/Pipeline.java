package macrobase.analysis.pipeline;

import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;

public interface Pipeline {
    void initialize(MacroBaseConf conf) throws Exception;
    AnalysisResult run() throws Exception;
}