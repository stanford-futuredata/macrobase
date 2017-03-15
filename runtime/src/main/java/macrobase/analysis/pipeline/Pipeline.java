package macrobase.analysis.pipeline;

import java.util.List;

import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;

/*
 Basic MacroBase query pipeline interface.
 Instances can be run programmatically via config option.
 */
public interface Pipeline {
    Pipeline initialize(MacroBaseConf conf) throws Exception;
    List<AnalysisResult> run() throws Exception;
}