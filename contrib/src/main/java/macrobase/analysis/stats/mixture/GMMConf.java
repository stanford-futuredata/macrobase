package macrobase.analysis.stats.mixture;

public class GMMConf {
    public static final String NUM_MIXTURES = "macrobase.analysis.stat.mixtures.numMixtures";
    public static final String MAX_ITERATIONS_TO_CONVERGE = "macrobase.analysis.stat.iterative.maxIterations";
    public static final String ITERATIVE_PROGRESS_CUTOFF_RATIO = "macrobase.analysis.stat.iterative.improvementCutoffRatio";
    public static final String DPM_TRUNCATING_PARAMETER = "macrobase.analysis.stat.dpm.truncatingParameter";
    public static final String DPM_CONCENTRATION_PARAMETER = "macrobase.analysis.stat.dpm.concentrationParameter";
    public static final String SVI_DELAY = "macrobase.analysis.stat.svi.delay";
    public static final String SVI_FORGETTING_RATE = "macrobase.analysis.stat.svi.forgettingRate";
    public static final String SVI_MINIBATCH_SIZE = "macrobase.analysis.stat.svi.minibatchSize";
    public static final String TRAIN_TEST_SPLIT = "macrobase.analysis.stat.trainTestSplit";
    public static final String MIXTURE_CENTERS_FILE = "macrobase.analysis.stat.mixtures.initialClusters";
    public static final String TARGET_GROUP = "macrobase.analysis.classify.targetGroup";
    public static final String SCORE_DUMP_FILE_CONFIG_PARAM = "macrobase.diagnostic.dumpScoreFile";

    public static final String DUMP_MIXTURE_COMPONENTS_DEFAULT = null;
    public static final Integer SVI_MINIBATCH_SIZE_DEFAULT = 10000;
    public static final Double SVI_DELAY_DEFAULT = 1.0;
    public static final Double SVI_FORGETTING_RATE_DEFAULT = 0.9;
    public static final Double TRAIN_TEST_SPLIT_DEFAULT = -1.0; // Train and test on the entire dataset while training
    public static final Integer NUM_MIXTURES_DEFAULT = 2;
    public static final Double ITERATIVE_PROGRESS_CUTOFF_RATIO_DEFAULT = 0.00001;
    public static final Integer DPM_TRUNCATING_PARAMETER_DEFAULT = 20;
    public static final Double DPM_CONCENTRATION_PARAMETER_DEFAULT = 1.;
    public static final Integer MAX_ITERATIONS_TO_CONVERGE_DEFAULT = 100;
}
