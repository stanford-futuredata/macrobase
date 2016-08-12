package macrobase.analysis.pipeline;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseConf.DataIngesterType;
import macrobase.conf.MacroBaseConf.TransformType;
import macrobase.conf.MacroBaseDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class BasePipeline implements Pipeline {
    private static Logger log = LoggerFactory.getLogger(BasePipeline.class);

    protected Boolean forceUsePercentile;
    protected Boolean forceUseZScore;
    protected Double minOIRatio;
    protected Double minSupport;
    protected Double targetPercentile;
    protected Double zScore;
    protected List<String> attributes;
    protected List<String> highMetrics;
    protected List<String> lowMetrics;
    protected String queryName;

    protected MacroBaseConf conf;

    @Override
    public Pipeline initialize(MacroBaseConf conf) throws Exception {
        this.conf = conf;

        queryName = conf.getString(MacroBaseConf.QUERY_NAME, MacroBaseDefaults.QUERY_NAME);
        log.debug("Running query {}", queryName);
        log.debug("CONFIG:\n{}", conf.toString());

        zScore = conf.getDouble(MacroBaseConf.ZSCORE, MacroBaseDefaults.ZSCORE);
        targetPercentile = conf.getDouble(MacroBaseConf.TARGET_PERCENTILE, MacroBaseDefaults.TARGET_PERCENTILE);
        minOIRatio = conf.getDouble(MacroBaseConf.MIN_OI_RATIO, MacroBaseDefaults.MIN_OI_RATIO);
        minSupport = conf.getDouble(MacroBaseConf.MIN_SUPPORT, MacroBaseDefaults.MIN_SUPPORT);
        forceUsePercentile = conf.getBoolean(MacroBaseConf.USE_PERCENTILE, MacroBaseDefaults.USE_PERCENTILE);
        forceUseZScore = conf.getBoolean(MacroBaseConf.USE_ZSCORE, MacroBaseDefaults.USE_ZSCORE);

        attributes = conf.getStringList(MacroBaseConf.ATTRIBUTES);
        lowMetrics = conf.getStringList(MacroBaseConf.LOW_METRICS);
        highMetrics = conf.getStringList(MacroBaseConf.HIGH_METRICS);

        return this;
    }
}
