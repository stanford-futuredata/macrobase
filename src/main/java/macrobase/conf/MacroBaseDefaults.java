package macrobase.conf;

import macrobase.analysis.outlier.KDE;
import macrobase.conf.MacroBaseConf.DataLoaderType;
import macrobase.conf.MacroBaseConf.DetectorType;
import macrobase.conf.MacroBaseConf.DataTransformType;

public class MacroBaseDefaults {
    public static final String QUERY_NAME = "MacroBaseQuery";

    // analysis defaults
    public static final Double ZSCORE = 3.;
    public static final Double TARGET_PERCENTILE = .99;
    public static final Double MIN_OI_RATIO = 3.;
    public static final Double MIN_SUPPORT = 0.001;
    public static final Long RANDOM_SEED = null;
    public static final Boolean USE_PERCENTILE = true;
    public static final Boolean USE_ZSCORE = false;

    // streaming defaults
    public static final Integer WARMUP_COUNT = 10000;
    public static final Integer INPUT_RESERVOIR_SIZE = 10000;
    public static final Integer SCORE_RESERVOIR_SIZE = 10000;
    public static final Integer SUMMARY_UPDATE_PERIOD = 100000;
    public static final Boolean USE_REAL_TIME_PERIOD = false;
    public static final Boolean USE_TUPLE_COUNT_PERIOD = true;
    public static final Double DECAY_RATE = .01;
    public static final Integer MODEL_UPDATE_PERIOD = 100000;
    public static final Integer OUTLIER_ITEM_SUMMARY_SIZE = 100000;
    public static final Integer INLIER_ITEM_SUMMARY_SIZE = 100000;

    // MCD defaults
    public static final Double MCD_ALPHA = 0.5;
    public static final Double MCD_STOPPING_DELTA = 1e-3;

    // KDE defaults
    public static final KDE.Bandwidth KDE_BANDWIDTH = KDE.Bandwidth.OVERSMOOTHED;
    public static final KDE.KernelType KDE_KERNEL_TYPE = KDE.KernelType.EPANECHNIKOV_MULTIPLICATIVE;

    // loader defaults
    public static final DataLoaderType DATA_LOADER_TYPE = DataLoaderType.POSTGRES_LOADER;
    public static final DetectorType DETECTOR_TYPE = DetectorType.MAD_OR_MCD;
    public static final DataTransformType DATA_TRANSFORM = DataTransformType.ZERO_TO_ONE_SCALE;

    public static final String DB_USER = System.getProperty("user.name");
    public static final String DB_PASSWORD = "";
    public static final String DB_NAME = "postgres";
    public static final String DB_URL = "localhost";
}
