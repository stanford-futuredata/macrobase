package macrobase.analysis.contextualoutlier.conf;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;

public class ContextualConf {
    public static final String CONTEXTUAL_API = "macrobase.analysis.contextual.api";
    public static final String CONTEXTUAL_API_OUTLIER_PREDICATES = "macrobase.analysis.contextual.api.outlierPredicates";
    public static final String CONTEXTUAL_DISCRETE_ATTRIBUTES = "macrobase.analysis.contextual.discreteAttributes";
    public static final String CONTEXTUAL_DOUBLE_ATTRIBUTES = "macrobase.analysis.contextual.doubleAttributes";
    public static final String CONTEXTUAL_DENSECONTEXTTAU = "macrobase.analysis.contextual.denseContextTau";
    public static final String CONTEXTUAL_NUMINTERVALS = "macrobase.analysis.contextual.numIntervals";
    public static final String CONTEXTUAL_MAX_PREDICATES = "macrobase.analysis.contextual.maxPredicates";
    public static final String CONTEXTUAL_OUTPUT_FILE = "macrobase.analysis.contextual.outputFile";
    public static final String CONTEXTUAL_PRUNING_DENSITY = "macrobase.analysis.contextual.pruning.density";
    public static final String CONTEXTUAL_PRUNING_DEPENDENCY = "macrobase.analysis.contextual.pruning.dependency";
    public static final String CONTEXTUAL_PRUNING_DISTRIBUTION_FOR_TRAINING = "macrobase.analysis.contextual.pruning.distributionForTraining";
    public static final String CONTEXTUAL_PRUNING_DISTRIBUTION_FOR_SCORING = "macrobase.analysis.contextual.pruning.distributionForScoring";

    public enum ContextualAPI {
        findAllContextualOutliers,
        findContextsGivenOutlierPredicate,
    }

    public static ContextualAPI getContextualAPI(MacroBaseConf conf) throws ConfigurationException {
        if (!conf.isSet(CONTEXTUAL_API)) {
            return ContextualDefaults.CONTEXTUAL_API;
        }
        return ContextualAPI.valueOf(conf.getString(CONTEXTUAL_API));
    }
}
