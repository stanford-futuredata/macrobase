package macrobase.analysis.contextualoutlier.conf;

import macrobase.conf.MacroBaseConf;

import java.util.ArrayList;
import java.util.List;


public class ContextualDefaults {
    //contextual outlier detection defaults
    public static final ContextualConf.ContextualAPI CONTEXTUAL_API = ContextualConf.ContextualAPI.findAllContextualOutliers;
    public static final String CONTEXTUAL_API_OUTLIER_PREDICATES  = "";
    public static final Double CONTEXTUAL_DENSECONTEXTTAU = 0.5;
    public static final Integer CONTEXTUAL_NUMINTERVALS = 10;
    public static final List<String> CONTEXTUAL_DISCRETE_ATTRIBUTES = new ArrayList<String>();
    public static final List<String> CONTEXTUAL_DOUBLE_ATTRIBUTES = new ArrayList<String>();
    public static final Integer CONTEXTUAL_MAX_PREDICATES = Integer.MAX_VALUE;
    public static final String CONTEXTUAL_OUTPUT_FILE = null;
    public static final Boolean CONTEXTUAL_PRUNING_DENSITY = true;
    public static final Boolean CONTEXTUAL_PRUNING_DEPENDENCY = true;
    public static final Boolean CONTEXTUAL_PRUNING_DISTRIBUTION_FOR_TRAINING = true;
    public static final Boolean CONTEXTUAL_PRUNING_DISTRIBUTION_FOR_SCORING = true;
}
