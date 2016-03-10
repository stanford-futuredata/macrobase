package macrobase.analysis.pipeline;

import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;

import java.io.IOException;
import java.sql.SQLException;


abstract public class OneShotPipeline extends AbstractPipeline {
    boolean ran = false;

    public OneShotPipeline(MacroBaseConf conf) throws ConfigurationException {
        super(conf);
    }

    abstract AnalysisResult run() throws SQLException, IOException, ConfigurationException;

    @Override
    public AnalysisResult next() {
        try {
            AnalysisResult result = run();
            ran = true;
            return result;
        } catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("analyze threw an exception");
        }
    }

    @Override
    public boolean hasNext() {
        return !ran;
    }
}
