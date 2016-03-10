package macrobase.analysis.pipeline;

import macrobase.analysis.classify.OutlierClassifier;
import macrobase.analysis.result.AnalysisResult;
import macrobase.analysis.summary.Summarizer;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseConf.DataIngesterType;
import macrobase.conf.MacroBaseConf.TransformType;
import macrobase.conf.MacroBaseDefaults;
import macrobase.ingest.DataIngester;
import macrobase.analysis.transform.FeatureTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public abstract class AbstractPipeline implements Iterator<AnalysisResult> {
    private static final Logger log = LoggerFactory.getLogger(AbstractPipeline.class);

    protected final Boolean forceUsePercentile;
    protected final Boolean forceUseZScore;
    protected final DataIngesterType dataIngesterType;
    protected final TransformType transformType;
    protected final Double contextualDenseContextTau;
    protected final Double minOIRatio;
    protected final Double minSupport;
    protected final Double targetPercentile;
    protected final Double zScore;
    protected final Integer contextualNumIntervals;
    protected final List<String> attributes;
    protected final List<String> contextualDiscreteAttributes;
    protected final List<String> contextualDoubleAttributes;
    protected final List<String> highMetrics;
    protected final List<String> lowMetrics;
    protected final MacroBaseConf conf;
    protected final String queryName;
    protected final String storeAnalysisResults;
    protected final boolean contextualEnabled;

    public AbstractPipeline(MacroBaseConf conf) throws ConfigurationException {
        this.conf = conf;

        queryName = conf.getString(MacroBaseConf.QUERY_NAME, MacroBaseDefaults.QUERY_NAME);
        log.debug("Running query {}", queryName);
        log.debug("CONFIG:\n{}", conf.toString());

        zScore = conf.getDouble(MacroBaseConf.ZSCORE, MacroBaseDefaults.ZSCORE);
        targetPercentile = conf.getDouble(MacroBaseConf.TARGET_PERCENTILE, MacroBaseDefaults.TARGET_PERCENTILE);
        minOIRatio = conf.getDouble(MacroBaseConf.MIN_OI_RATIO, MacroBaseDefaults.MIN_OI_RATIO);
        minSupport = conf.getDouble(MacroBaseConf.MIN_SUPPORT, MacroBaseDefaults.MIN_SUPPORT);
        transformType = conf.getTransformType();
        forceUsePercentile = conf.getBoolean(MacroBaseConf.USE_PERCENTILE, MacroBaseDefaults.USE_PERCENTILE);
        forceUseZScore = conf.getBoolean(MacroBaseConf.USE_ZSCORE, MacroBaseDefaults.USE_ZSCORE);

        dataIngesterType = conf.getDataLoaderType();
        attributes = conf.getStringList(MacroBaseConf.ATTRIBUTES);
        lowMetrics = conf.getStringList(MacroBaseConf.LOW_METRICS);
        highMetrics = conf.getStringList(MacroBaseConf.HIGH_METRICS);

        storeAnalysisResults = conf.getString(MacroBaseConf.STORE_ANALYSIS_RESULTS, MacroBaseDefaults.STORE_ANALYSIS_RESULTS);

        contextualEnabled = conf.getBoolean(MacroBaseConf.CONTEXTUAL_ENABLED, MacroBaseDefaults.CONTEXTUAL_ENABLED);
        contextualDiscreteAttributes = conf.getStringList(MacroBaseConf.CONTEXTUAL_DISCRETE_ATTRIBUTES,MacroBaseDefaults.CONTEXTUAL_DISCRETE_ATTRIBUTES);
        contextualDoubleAttributes = conf.getStringList(MacroBaseConf.CONTEXTUAL_DOUBLE_ATTRIBUTES,MacroBaseDefaults.CONTEXTUAL_DOUBLE_ATTRIBUTES);
        contextualDenseContextTau = conf.getDouble(MacroBaseConf.CONTEXTUAL_DENSECONTEXTTAU, MacroBaseDefaults.CONTEXTUAL_DENSECONTEXTTAU);
        contextualNumIntervals = conf.getInt(MacroBaseConf.CONTEXTUAL_NUMINTERVALS, MacroBaseDefaults.CONTEXTUAL_NUMINTERVALS);
    }

    @Override
    public abstract AnalysisResult next();

    @Override
    public abstract boolean hasNext();

}
