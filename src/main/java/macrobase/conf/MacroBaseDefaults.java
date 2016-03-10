package macrobase.conf;

import macrobase.analysis.stats.KDE;
import macrobase.conf.MacroBaseConf.DataIngesterType;
import macrobase.ingest.CSVIngester;

import java.util.ArrayList;
import java.util.List;

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
    public static final Double SUMMARY_UPDATE_PERIOD = 100000.;
    public static final MacroBaseConf.PeriodType DECAY_TYPE = MacroBaseConf.PeriodType.TUPLE_BASED;
    public static final Double DECAY_RATE = .01;
    public static final Double MODEL_UPDATE_PERIOD = 100000.;
    public static final Integer OUTLIER_ITEM_SUMMARY_SIZE = 100000;
    public static final Integer INLIER_ITEM_SUMMARY_SIZE = 100000;
    
    // timeseries defaults
    public static final Integer TUPLE_WINDOW = 100;

    // MCD defaults
    public static final Double MCD_ALPHA = 0.5;
    public static final Double MCD_STOPPING_DELTA = 1e-3;

    // KDE defaults
    public static final Double KDE_BANDWIDTH_MULTIPLIER = 1.0;
    public static final KDE.BandwidthAlgorithm KDE_BANDWIDTH_ALGORITHM = KDE.BandwidthAlgorithm.OVERSMOOTHED;
    public static final KDE.KernelType KDE_KERNEL_TYPE = KDE.KernelType.EPANECHNIKOV_MULTIPLICATIVE;

    // BinnedKDE defaults
    public static final Integer BINNED_KDE_BINS = 10000;

    // TreeKDE defaults
    public static final Integer KDTREE_LEAF_CAPACITY = 2;
    public static final Double TREE_KDE_ACCURACY = 1e-5;

    public static final String R_LOG_FILE = null;
    // Analysis results
    public static final String STORE_ANALYSIS_RESULTS = null;

    // loader defaults
    public static final DataIngesterType DATA_LOADER_TYPE = DataIngesterType.POSTGRES_LOADER;
    public static final String TIME_COLUMN = null;
    public static final MacroBaseConf.TransformType TRANSFORM_TYPE = MacroBaseConf.TransformType.MAD_OR_MCD;
    public static final CSVIngester.Compression CSV_COMPRESSION = CSVIngester.Compression.UNCOMPRESSED;

    public static final String DB_USER = System.getProperty("user.name");
    public static final String DB_PASSWORD = "";
    public static final String DB_NAME = "postgres";
    public static final String DB_URL = "localhost";
    
    public static final Boolean CONTEXTUAL_ENABLED = false;
    public static final Double CONTEXTUAL_DENSECONTEXTTAU = 0.5;
    public static final Integer CONTEXTUAL_NUMINTERVALS = 10;
    public static final List<String> CONTEXTUAL_DISCRETE_ATTRIBUTES = new ArrayList<String>();
    public static final List<String> CONTEXTUAL_DOUBLE_ATTRIBUTES = new ArrayList<String>();
    public static final Double OUTLIER_STATIC_THRESHOLD = 3.0;
}
