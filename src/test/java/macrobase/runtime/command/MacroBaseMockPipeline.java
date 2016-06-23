package macrobase.runtime.command;

import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;

import java.util.ArrayList;
import java.util.List;

class MacroBaseMockPipeline implements Pipeline {
    static boolean initialized = false;
    static boolean ran = false;

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        initialized = true;
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        ran = true;
        return new ArrayList<>();
    }
}
