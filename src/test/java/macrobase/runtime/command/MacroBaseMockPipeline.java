package macrobase.runtime.command;

import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.itemset.result.ItemsetResult;
import macrobase.conf.MacroBaseConf;

import java.util.ArrayList;
import java.util.List;

class MacroBaseMockPipeline implements Pipeline {
    static boolean initialized = false;
    static boolean ran = false;

    boolean returnMany = false;

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        initialized = true;
        return this;
    }

    @Override
    public List<AnalysisResult> run() throws Exception {
        ran = true;

        if(returnMany) {
            List<AnalysisResult> ret = new ArrayList<>();
            for(int i = 0; i < 2000; ++i) {
                ret.add(new AnalysisResult(0, 0, 0, 0, 0, new ArrayList<ItemsetResult>()));
            }

            return ret;
        }

        return new ArrayList<>();
    }
}
