package macrobase.analysis.pipeline;

import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;

/*
 Basic MacroBase query pipeline interface.
 Instances can be run programmatically via config option.
 */
public interface Pipeline {
    Pipeline initialize(MacroBaseConf conf) throws Exception;
    AnalysisResult run() throws Exception;
}