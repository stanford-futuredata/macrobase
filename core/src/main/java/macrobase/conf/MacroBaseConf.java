package macrobase.conf;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.dropwizard.Configuration;
import macrobase.analysis.stats.*;
import macrobase.analysis.stats.mixture.*;
import macrobase.analysis.transform.aggregate.*;
import macrobase.ingest.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class MacroBaseConf extends Configuration {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseConf.class);

    public static final String QUERY_NAME = "macrobase.query.name";
    public static final String PIPELINE_NAME = "macrobase.pipeline.class";

    public static final String ZSCORE = "macrobase.analysis.zscore.threshold";
    public static final String TARGET_PERCENTILE = "macrobase.analysis.targetPercentile";
    public static final String MIN_SUPPORT = "macrobase.analysis.minSupport";
    public static final String MIN_OI_RATIO = "macrobase.analysis.minOIRatio";
    public static final String ATTRIBUTE_COMBINATIONS = "macrobase.analysis.summary.findAttributeCombinations";

    public static final String RANDOM_SEED = "macrobase.analysis.randomSeed";
    public static final String USE_PERCENTILE = "macrobase.analysis.usePercentile";
    public static final String USE_ZSCORE = "macrobase.analysis.useZScore";
    public static final String TRANSFORM_TYPE = "macrobase.analysis.transformType";
    public static final String AGGREGATE_TYPE = "macrobase.analysis.aggregateType";

    public static final String WARMUP_COUNT = "macrobase.analysis.streaming.warmupCount";
    public static final String TUPLE_BATCH_SIZE = "macrobase.analysis.streaming.tupleBatchSize";
    public static final String INPUT_RESERVOIR_SIZE = "macrobase.analysis.streaming.inputReservoirSize";
    public static final String SCORE_RESERVOIR_SIZE = "macrobase.analysis.streaming.scoreReservoirSize";
    public static final String SUMMARY_UPDATE_PERIOD = "macrobase.analysis.streaming.summaryUpdatePeriod";
    public static final String DECAY_TYPE = "macrobase.analysis.streaming.decayType";
    public static final String DECAY_RATE = "macrobase.analysis.streaming.decayRate";
    public static final String MODEL_UPDATE_PERIOD = "macrobase.analysis.streaming.modelUpdatePeriod";
    public static final String OUTLIER_ITEM_SUMMARY_SIZE = "macrobase.analysis.streaming.outlierSummarySize";
    public static final String INLIER_ITEM_SUMMARY_SIZE = "macrobase.analysis.streaming.inlierItemSummarySize";

    public static final String TUPLE_WINDOW = "macrobase.analysis.timeseries.tupleWindow";
    public static final String WINDOW_SIZE = "macrobase.analysis.timeseries.windowSize";

    public static final String MCD_ALPHA = "macrobase.analysis.mcd.alpha";
    public static final String MCD_STOPPING_DELTA = "macrobase.analysis.mcd.stoppingDelta";

    public static final String NUM_MIXTURES = "macrobase.analysis.stat.mixtures.numMixtures";
    public static final String MAX_ITERATIONS_TO_CONVERGE = "macrobase.analysis.stat.iterative.maxIterations";
    public static final String ITERATIVE_PROGRESS_CUTOFF_RATIO = "macrobase.analysis.stat.iterative.improvementCutoffRatio";
    public static final String DPM_TRUNCATING_PARAMETER = "macrobase.analysis.stat.dpm.truncatingParameter";
    public static final String DPM_CONCENTRATION_PARAMETER = "macrobase.analysis.stat.dpm.concentrationParameter";
    public static final String SVI_DELAY = "macrobase.analysis.stat.svi.delay";
    public static final String SVI_FORGETTING_RATE = "macrobase.analysis.stat.svi.forgettingRate";
    public static final String SVI_MINIBATCH_SIZE = "macrobase.analysis.stat.svi.minibatchSize";

    // Algorithm to use when choosing the bandwidth for the given data.
    public static final String KDE_BANDWIDTH_ALGORITHM = "macrobase.analysis.kde.bandwidthAlgorithm";
    public static final String KDE_PROPORTION_OF_DATA_TO_USE = "macrobase.analysis.kde.proportionOfDataToUse";

    // Multiplies the bandwidth that was gotten algorithmically by this given constant (double).
    public static final String KDE_BANDWIDTH_MULTIPLIER = "macrobase.analysis.kde.bandwidthMultiplier";
    public static final String KDE_KERNEL_TYPE = "macrobase.analysis.kde.kernelType";
    public static final String BINNED_KDE_BINS = "macrobase.analysis.binnedKde.numBins";
    public static final String KDTREE_LEAF_CAPACITY = "macrobase.analysis.treeKde.leafCapacity";
    public static final String TREE_KDE_ACCURACY = "macrobase.analysis.treeKde.accuracy";

    public static final String RANDOM_PROJECTION_K = "macrobase.analysis.randomProjection.k";

    public static final String TRUNCATE_K = "macrobase.analysis.truncate.k";

    public static final String R_LOG_FILE = "macrobase.analysis.r.logfile";
    public static final String STORE_ANALYSIS_RESULTS = "macrobase.analysis.results.store";

    public static final String DATA_LOADER_TYPE = "macrobase.loader.loaderType";
    public static final String TIME_COLUMN = "macrobase.loader.timeColumn";
    public static final String ATTRIBUTES = "macrobase.loader.attributes";
    public static final String LOW_METRICS = "macrobase.loader.targetLowMetrics";
    public static final String HIGH_METRICS = "macrobase.loader.targetHighMetrics";
    public static final String AUXILIARY_ATTRIBUTES = "macrobase.loader.auxiliaryAttributes";


    public static final String JDBC_PROPERTIES = "macrobase.ingest.jdbc.properties";

    public static final String BASE_QUERY = "macrobase.loader.db.baseQuery";
    public static final String DB_USER = "macrobase.loader.db.user";
    public static final String DB_PASSWORD = "macrobase.loader.db.password";
    public static final String DB_NAME = "macrobase.loader.db.database";
    public static final String DB_URL = "macrobase.loader.db.url";
    public static final String DB_CACHE_DIR = "macrobase.loader.db.cacheDirectory";
    public static final String DB_CACHE_CHUNK_SIZE = "macrobase.loader.db.cacheChunkSizeTuples";


    public static final String CSV_INPUT_FILE = "macrobase.loader.csv.file";
    public static final String CSV_COMPRESSION = "macrobase.loader.csv.compression";

    // Queries given as a JSON string. Example:
    // {
    //   "queries": [
    //     {
    //       "project": "my-project",
    //       "filter": "metric.type=\"custom.googleapis.com/test\"",
    //       "alignmentPeriod": "300s",
    //       "perSeriesAligner": "ALIGN_MEAN",
    //       "crossSeriesReducer": "REDUCE_NONE",
    //       "groupByFields": []
    //     }
    //   ]
    // }
    public static final String GOOGLE_MONITORING_QUERIES = "macrobase.loader.googlemonitoring.queries";
    // Start and end times given in RFC3339 format. Example: "2016-08-08T12:00:00.0000Z"
    public static final String GOOGLE_MONITORING_START_TIME = "macrobase.loader.googlemonitoring.startTime";
    public static final String GOOGLE_MONITORING_END_TIME = "macrobase.loader.googlemonitoring.endTime";

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
    public static final String OUTLIER_STATIC_THRESHOLD = "macrobase.analysis.classify.outlierStaticThreshold";

    public static final String TARGET_GROUP = "macrobase.analysis.classify.targetGroup";
    public static final String SCORE_DUMP_FILE_CONFIG_PARAM = "macrobase.diagnostic.dumpScoreFile";
    public static final String CLASSIFIER_DUMP = "macrobase.diagnostic.dumpClassifier";
    public static final String DUMP_SCORE_GRID = "macrobase.diagnostic.dumpScoreGrid";
    public static final String NUM_SCORE_GRID_POINTS_PER_DIMENSION = "macrobase.diagnostic.gridPointsPerDimension";
    public static final String SCORED_DATA_FILE = "macrobase.diagnostic.scoreDataFile";
    public static final String DUMP_MIXTURE_COMPONENTS = "macrobase.diagnostic.dumpMixtureComponents";
    public static final String MIXTURE_CENTERS_FILE = "macrobase.analysis.stat.mixtures.initialClusters";
    public static final String TRAIN_TEST_SPLIT = "macrobase.analysis.stat.trainTestSplit";

    private final DatumEncoder datumEncoder;

    public MacroBaseConf() {
        datumEncoder = new DatumEncoder();
        _conf = new HashMap<>();
    }

    public DataIngester constructIngester() throws ConfigurationException, SQLException, IOException {
        DataIngesterType ingesterType = getDataLoaderType();
        if (ingesterType == DataIngesterType.CSV_LOADER) {
            return new CSVIngester(this);
        } else if (ingesterType == DataIngesterType.POSTGRES_LOADER) {
            return new PostgresIngester(this);
        } else if (ingesterType == DataIngesterType.CACHING_POSTGRES_LOADER) {
            return new DiskCachingIngester(this, new PostgresIngester(this));
        } else if (ingesterType == DataIngesterType.MYSQL_LOADER) {
            return new MySQLIngester(this);
        } else if (ingesterType == DataIngesterType.CACHING_MYSQL_LOADER) {
            return new DiskCachingIngester(this, new MySQLIngester(this));
        } else if (ingesterType == DataIngesterType.GOOGLE_MONITORING_LOADER) {
            return new GoogleMonitoringIngester(this);
        }

        throw new ConfigurationException(String.format("Unknown data loader type: %s", ingesterType));
    }

    public DatumEncoder getEncoder() {
        return datumEncoder;
    }

    public enum PeriodType {
        TUPLE_BASED,
        TIME_BASED
    }

    public enum TransformType {
        MAD_OR_MCD,
        MAD,
        MCD,
        ZSCORE,
        KDE,
        BINNED_KDE,
        TREE_KDE,
        MOVING_AVERAGE,
        ARIMA,
        BAYESIAN_NORMAL,
        EM_GMM,
        MEAN_FIELD_GMM,
        MEAN_FIELD_DPGMM,
        SVI_GMM,
        SVI_DPGMM,
    }

    public enum ContextualAPI {
        findAllContextualOutliers,
        findContextsGivenOutlierPredicate,
    }

    public enum AggregateType {
        COUNT,
        SUM,
        MAX
    }

    public Random getRandom() {
        Long seed = getLong(RANDOM_SEED, null);
        if (seed != null) {
            return new Random(seed);
        } else {
            return new Random();
        }
    }

    public BatchTrainScore constructTransform(TransformType transformType)
            throws ConfigurationException {
        switch (transformType) {
            case MAD_OR_MCD:
                int metricsDimensions = this.getStringList(LOW_METRICS).size() + this.getStringList(HIGH_METRICS).size();
                if (metricsDimensions == 1) {
                    log.info("By default: using MAD transform for dimension 1 metric.");
                    return new MAD(this);
                } else {
                    log.info("By default: using MCD transform for dimension {} metrics.", metricsDimensions);
                    MinCovDet ret = new MinCovDet(this);
                    return ret;
                }
            case MAD:
                log.info("Using MAD transform.");
                return new MAD(this);
            case MCD:
                log.info("Using MCD transform.");
                MinCovDet ret = new MinCovDet(this);
                return ret;
            case ZSCORE:
                log.info("Using ZScore transform.");
                return new ZScore(this);
            case KDE:
                log.info("Using KDE transform.");
                return new KDE(this);
            case BINNED_KDE:
                log.info("Using BinnedKDE transform.");
                return new BinnedKDE(this);
            case TREE_KDE:
                log.info("Using TreeKDE transform.");
                return new TreeKDE(this);
            case MOVING_AVERAGE:
                log.info("Using Moving Average transform.");
                return new MovingAverage(this);
            case BAYESIAN_NORMAL:
                log.info("Using Bayesian Normal transform.");
                return new BayesianNormalDensity(this);
            case EM_GMM:
                log.info("Using Finite mixture of Gaussians (EM algorithm) transform.");
                return new ExpectMaxGMM(this);
            case MEAN_FIELD_GMM:
                log.info("Using Finite mixture of Gaussians (Bayesian algorithm) transform.");
                return new FiniteGMM(this);
            case MEAN_FIELD_DPGMM:
                log.info("Using infinite mixture of Gaussians (DP Bayesian algorithm) transform.");
                return new DPGMM(this);
            case SVI_GMM:
                log.info("Using infinite mixture of Gaussians (DP Bayesian algorithm) transform.");
                return new StochVarFiniteGMM(this);
            case SVI_DPGMM:
                log.info("Using infinite mixture of Gaussians (DP Bayesian algorithm) transform.");
                return new StochVarDPGMM(this);
            default:
                throw new RuntimeException("Unhandled transform class!" + transformType);
        }
    }

    public BatchWindowAggregate constructBatchAggregate(AggregateType aggregateType)
            throws ConfigurationException {
        switch (aggregateType) {
            case MAX:
                log.info("Using MAX aggregation.");
                return new BatchWindowMax(this);
            default:
                throw new RuntimeException("Unhandled batch aggreation type!" + aggregateType);
        }
    }

    public IncrementalWindowAggregate constructIncrementalAggregate(AggregateType aggregateType)
            throws ConfigurationException {
        switch (aggregateType) {
            case SUM:
                log.info("Using SUM aggregation.");
                return new IncrementalWindowSum(this);
            case COUNT:
                log.info("Using COUNT aggregation.");
                return new IncrementalWindowCount(this);
            default:
                throw new RuntimeException("Unhandled incremental aggreation type!" + aggregateType);
        }
    }

    public enum DataIngesterType {
        CSV_LOADER,
        POSTGRES_LOADER,
        CACHING_POSTGRES_LOADER,
        MYSQL_LOADER,
        CACHING_MYSQL_LOADER,
        GOOGLE_MONITORING_LOADER,
    }


    private Map<String, String> _conf;

    @JsonAnySetter
    public MacroBaseConf set(String key, Object value) {
        if (value instanceof List) {
            value = ((List) value).stream().collect(Collectors.joining(","));
        }

        _conf.put(key, value.toString());
        return this;
    }

    public Boolean isSet(String key) {
        return _conf.containsKey(key);
    }

    public String getString(String key) throws ConfigurationException {
        if (!_conf.containsKey(key)) {
            throw new MissingParameterException(key);
        }
        return _conf.get(key);
    }

    public String getString(String key, String defaultValue) {
        return _conf.getOrDefault(key, defaultValue);
    }

    public List<String> getStringList(String key) throws ConfigurationException {
        if (!_conf.containsKey(key)) {
            throw new MissingParameterException(key);
        }
        return getStringList(key, null);
    }

    public List<String> getStringList(String key, List<String> defaultValue) {
        if (_conf.containsKey(key)) {
            return _conf.get(key).length() > 0 ? Arrays.asList(_conf.get(key).split(",[ ]*")) : new ArrayList<>();
        }
        return defaultValue;
    }

    public Double getDouble(String key) throws ConfigurationException {
        if (!_conf.containsKey(key)) {
            throw new MissingParameterException(key);
        }
        return getDouble(key, null);
    }

    public Double getDouble(String key, Double defaultValue) {
        if (_conf.containsKey(key)) {
            return Double.parseDouble(_conf.get(key));
        }
        return defaultValue;
    }

    public Map<String, String> getMap(String key) {
        if (!_conf.containsKey(key)) {
            return new HashMap<>();
        }

        Map<String, String> properties = new HashMap<>();
        for (String property : Arrays.asList(_conf.get(key).split(", |[{}]"))) {
            if (property.isEmpty()) {
                continue;
            }
            String[] keyValue = property.split("=", 2);
            properties.put(keyValue[0], keyValue[1]);
        }
        return properties;
    }

    public List<Double> getDoubleList(String key) throws ConfigurationException {
        if (!_conf.containsKey(key)) {
            throw new MissingParameterException(key);
        }
        return _conf.get(key).length() > 0 ? Arrays.asList(_conf.get(key).split(",[ ]*")).stream().map(Double::parseDouble).collect(Collectors.toList()) : new ArrayList<>();
    }

    public List<Double> getDoubleList(String key, List<Double> defaultValue) throws ConfigurationException {
        if (_conf.containsKey(key)) {
            return getDoubleList(key);
        }
        return defaultValue;
    }

    public Integer getInt(String key) throws ConfigurationException {
        if (!_conf.containsKey(key)) {
            throw new MissingParameterException(key);
        }
        return getInt(key, null);
    }

    public Integer getInt(String key, Integer defaultValue) {
        if (_conf.containsKey(key)) {
            return Integer.parseInt(_conf.get(key));
        }
        return defaultValue;
    }

    public Long getLong(String key) throws ConfigurationException {
        if (!_conf.containsKey(key)) {
            throw new MissingParameterException(key);
        }
        return getLong(key, null);
    }

    public Long getLong(String key, Long defaultValue) {
        if (_conf.containsKey(key)) {
            return Long.parseLong(_conf.get(key));
        }
        return defaultValue;
    }

    public Boolean getBoolean(String key) throws ConfigurationException {
        if (_conf.containsKey(key) || MacroBaseDefaults.DEFAULT_BOOLEANS.containsKey(key)) {
            return getBoolean(
                    key,
                    MacroBaseDefaults.DEFAULT_BOOLEANS.get(key)
            );
        } else {
            throw new MissingParameterException(key);
        }
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        if (_conf.containsKey(key)) {
            return Boolean.parseBoolean(_conf.get(key));
        }
        return defaultValue;
    }

    public DataIngesterType getDataLoaderType() throws ConfigurationException {
        if (!_conf.containsKey(DATA_LOADER_TYPE)) {
            return MacroBaseDefaults.DATA_LOADER_TYPE;
        }
        return DataIngesterType.valueOf(_conf.get(DATA_LOADER_TYPE));
    }

    public TransformType getTransformType() throws ConfigurationException {
        if (!_conf.containsKey(TRANSFORM_TYPE)) {
            return MacroBaseDefaults.TRANSFORM_TYPE;
        }
        return TransformType.valueOf(_conf.get(TRANSFORM_TYPE));
    }

    public ContextualAPI getContextualAPI() throws ConfigurationException {
        if (!_conf.containsKey(CONTEXTUAL_API)) {
            return MacroBaseDefaults.CONTEXTUAL_API;
        }
        return ContextualAPI.valueOf(_conf.get(CONTEXTUAL_API));
    }

    public AggregateType getAggregateType() throws ConfigurationException {
        if (!_conf.containsKey(AGGREGATE_TYPE)) {
            return MacroBaseDefaults.AGGREGATE_TYPE;
        }
        return AggregateType.valueOf(_conf.get(AGGREGATE_TYPE));
    }

    public KDE.BandwidthAlgorithm getKDEBandwidth() {
        if (!_conf.containsKey(KDE_BANDWIDTH_ALGORITHM)) {
            return MacroBaseDefaults.KDE_BANDWIDTH_ALGORITHM;
        }
        return KDE.BandwidthAlgorithm.valueOf(_conf.get(KDE_BANDWIDTH_ALGORITHM));
    }

    public KDE.KernelType getKDEKernelType() {
        if (!_conf.containsKey(KDE_KERNEL_TYPE)) {
            return MacroBaseDefaults.KDE_KERNEL_TYPE;
        }
        return KDE.KernelType.valueOf(_conf.get(KDE_KERNEL_TYPE));
    }

    public CSVIngester.Compression getCsvCompression() {
        if (!_conf.containsKey(CSV_COMPRESSION)) {
            return MacroBaseDefaults.CSV_COMPRESSION;
        }
        return CSVIngester.Compression.valueOf(_conf.get(CSV_COMPRESSION));
    }

    public PeriodType getDecayType() {
        if (!_conf.containsKey(DECAY_TYPE)) {
            return MacroBaseDefaults.DECAY_TYPE;
        }
        return PeriodType.valueOf(_conf.get(DECAY_TYPE));
    }

    @Override
    public String toString() {
        return _conf.entrySet().stream()
                .sorted((a, b) -> a.getKey().compareTo(b.getKey()))
                .map(e -> e.getKey() + ": " + e.getValue())
                .collect(Collectors.joining("\n"));
    }

    private void sanityCheckBase() throws ConfigurationException {
        if (getBoolean(USE_PERCENTILE, false) && getBoolean(USE_ZSCORE, false)) {
            throw new ConfigurationException(String.format("Can only select one of %s or %s",
                    USE_PERCENTILE,
                    USE_ZSCORE));
        } else if (!(getBoolean(USE_PERCENTILE, false) || getBoolean(USE_ZSCORE, false))) {
            throw new ConfigurationException(String.format("Must select one of %s or %s",
                    USE_PERCENTILE,
                    USE_ZSCORE));
        }
    }

    public void sanityCheckBatch() throws ConfigurationException {
        sanityCheckBase();
    }

    public void sanityCheckStreaming() throws ConfigurationException {
        sanityCheckBase();
    }

    public void loadSystemProperties() {
        System.getProperties().stringPropertyNames()
                .stream()
                .filter(e -> e.startsWith("macrobase"))
                .forEach(e -> set(e, System.getProperty(e)));
    }
}
