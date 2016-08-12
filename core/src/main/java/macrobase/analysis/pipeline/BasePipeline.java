package macrobase.analysis.pipeline;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class BasePipeline implements Pipeline {
    private static Logger log = LoggerFactory.getLogger(BasePipeline.class);

    protected MacroBaseConf conf;

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        this.conf = conf;

        String queryName = conf.getString(MacroBaseConf.QUERY_NAME, MacroBaseDefaults.QUERY_NAME);
        log.debug("Running query {}", queryName);
        log.debug("CONFIG:\n{}", conf.toString());

        return this;
    }
}
