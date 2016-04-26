package macrobase.analysis.pipeline;

import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;

import java.io.IOException;
import java.sql.SQLException;


abstract public class OneShotPipeline extends AbstractPipeline {
    public OneShotPipeline(MacroBaseConf conf) throws ConfigurationException {
        super(conf);
    }

    public abstract AnalysisResult run() throws Exception;
}
