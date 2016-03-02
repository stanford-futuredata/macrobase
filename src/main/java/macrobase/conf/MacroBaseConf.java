package macrobase.conf;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.dropwizard.Configuration;
import macrobase.analysis.outlier.KDE;
import macrobase.ingest.CsvLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MacroBaseConf extends Configuration {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseConf.class);

    public static final String QUERY_NAME = "macrobase.query.name";

    public static final String ZSCORE = "macrobase.analysis.zscore.threshold";
    public static final String TARGET_PERCENTILE = "macrobase.analysis.targetPercentile";
    public static final String MIN_SUPPORT = "macrobase.analysis.minSupport";
    public static final String MIN_OI_RATIO = "macrobase.analysis.minOIRatio";
    public static final String RANDOM_SEED = "macrobase.analysis.randomSeed";
    public static final String USE_PERCENTILE = "macrobase.analysis.usePercentile";
    public static final String USE_ZSCORE = "macrobase.analysis.useZScore";
    public static final String DETECTOR_TYPE = "macrobase.analysis.detectorType";

    public static final String WARMUP_COUNT = "macrobase.analysis.streaming.warmupCount";
    public static final String INPUT_RESERVOIR_SIZE = "macrobase.analysis.streaming.inputReservoirSize";
    public static final String SCORE_RESERVOIR_SIZE = "macrobase.analysis.streaming.scoreReservoirSize";
    public static final String SUMMARY_UPDATE_PERIOD = "macrobase.analysis.streaming.summaryUpdatePeriod";
    public static final String USE_REAL_TIME_PERIOD = "macrobase.analysis.streaming.useRealTimePeriod";
    public static final String USE_TUPLE_COUNT_PERIOD = "macrobase.analysis.streaming.useTupleCountPeriod";
    public static final String DECAY_RATE = "macrobase.analysis.streaming.decayRate";
    public static final String MODEL_UPDATE_PERIOD = "macrobase.analysis.streaming.modelUpdatePeriod";
    public static final String OUTLIER_ITEM_SUMMARY_SIZE = "macrobase.analysis.streaming.outlierSummarySize";
    public static final String INLIER_ITEM_SUMMARY_SIZE = "macrobase.analysis.streaming.inlierItemSummarySize";
    
    public static final String TUPLE_WINDOW = "macrobase.analysis.timeseries.tupleWindow";

    public static final String MCD_ALPHA = "macrobase.analysis.mcd.alpha";
    public static final String MCD_STOPPING_DELTA = "macrobase.analysis.mcd.stoppingDelta";

    // Algorithm to use when choosing the bandwidth for the given data.
    public static final String KDE_BANDWIDTH_ALGORITHM = "macrobase.analysis.kde.bandwidthAlgorithm";
    // Multiplies the bandwidth that was gotten algorithmically by this given constant (double).
    public static final String KDE_BANDWIDTH_MULTIPLIER = "macrobase.analysis.kde.bandwidthMultiplier";
    public static final String KDE_KERNEL_TYPE = "macrobase.analysis.kde.kernelType";
    public static final String BINNED_KDE_BINS = "macrobase.analysis.binnedKde.numBins";
    public static final String KDTREE_LEAF_CAPACITY = "macrobase.analysis.treeKde.leafCapacity";
    public static final String TREE_KDE_ACCURACY = "macrobase.analysis.treeKde.accuracy";

    public static final String STORE_ANALYSIS_RESULTS = "macrobase.analysis.results.store";

    public static final String DATA_TRANSFORM_TYPE = "macrobase.loader.transformType";
    public static final String DATA_LOADER_TYPE = "macrobase.loader.loaderType";
    public static final String TIME_COLUMN = "macrobase.loader.timeColumn";
    public static final String ATTRIBUTES = "macrobase.loader.attributes";
    public static final String LOW_METRICS = "macrobase.loader.targetLowMetrics";
    public static final String HIGH_METRICS = "macrobase.loader.targetHighMetrics";
    public static final String AUXILIARY_ATTRIBUTES = "macrobase.loader.auxiliaryAttributes";

    public static final String BASE_QUERY = "macrobase.loader.db.baseQuery";
    public static final String DB_USER = "macrobase.loader.db.user";
    public static final String DB_PASSWORD = "macrobase.loader.db.password";
    public static final String DB_NAME = "macrobase.loader.db.database";
    public static final String DB_URL = "macrobase.loader.db.url";
    public static final String DB_CACHE_DIR = "macrobase.loader.db.cacheDirectory";

    public static final String CSV_INPUT_FILE = "macrobase.loader.csv.file";
    public static final String CSV_COMPRESSION = "macrobase.loader.csv.compression";
    
    public static final String CONTEXTUAL_ENABLED = "macrobase.analysis.contextual.enabled";
    public static final String CONTEXTUAL_DISCRETE_ATTRIBUTES = "macrobase.analysis.contextual.discreteAttributes";
    public static final String CONTEXTUAL_DOUBLE_ATTRIBUTES = "macrobase.analysis.contextual.doubleAttributes";
    public static final String CONTEXTUAL_DENSECONTEXTTAU = "macrobase.analysis.contextual.denseContextTau";
    public static final String CONTEXTUAL_NUMINTERVALS = "macrobase.analysis.contextual.numIntervals";

    public enum DetectorType {
        MAD_OR_MCD,
        MAD,
        MCD,
        ZSCORE,
        KDE,
        BINNED_KDE,
        TREE_KDE,
        MOVING_AVERAGE
    }

    public enum DataLoaderType {
        CSV_LOADER,
        POSTGRES_LOADER,
        CACHING_POSTGRES_LOADER
    }

    public enum DataTransformType {
        ZERO_TO_ONE_SCALE,
        IDENTITY
    }

    private Map<String, String> _conf = new HashMap<>();

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
        if (!_conf.containsKey(key)) {
            throw new MissingParameterException(key);
        }
        return getBoolean(key, null);
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        if (_conf.containsKey(key)) {
            return Boolean.parseBoolean(_conf.get(key));
        }
        return defaultValue;
    }

    public DataLoaderType getDataLoaderType() throws ConfigurationException {
        if (!_conf.containsKey(DATA_LOADER_TYPE)) {
            return MacroBaseDefaults.DATA_LOADER_TYPE;
        }
        return DataLoaderType.valueOf(_conf.get(DATA_LOADER_TYPE));
    }

    public DetectorType getDetectorType() throws ConfigurationException {
        if (!_conf.containsKey(DETECTOR_TYPE)) {
            return MacroBaseDefaults.DETECTOR_TYPE;
        }
        return DetectorType.valueOf(_conf.get(DETECTOR_TYPE));
    }

    public DataTransformType getDataTransform() throws ConfigurationException {
        if (!_conf.containsKey(DATA_TRANSFORM_TYPE)) {
            return MacroBaseDefaults.DATA_TRANSFORM;
        }
        return DataTransformType.valueOf(_conf.get(DATA_TRANSFORM_TYPE));
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

    public CsvLoader.Compression getCsvCompression() {
        if (!_conf.containsKey(CSV_COMPRESSION)) {
            return MacroBaseDefaults.CSV_COMPRESSION;
        }
        return CsvLoader.Compression.valueOf(_conf.get(CSV_COMPRESSION));
    }



    @Override
    public String toString() {
        return _conf.entrySet().stream()
                .sorted((a, b) -> a.getKey().compareTo(b.getKey()))
                .map(e -> e.getKey() + ": " + e.getValue())
                .collect(Collectors.joining("\n"));
    }

    private void sanityCheckBase() throws ConfigurationException {
        if(getBoolean(USE_PERCENTILE, false) && getBoolean(USE_ZSCORE, false)) {
            throw new ConfigurationException(String.format("Can only select one of %s or %s",
                                                           USE_PERCENTILE,
                                                           USE_ZSCORE));
        }
        else if(!(getBoolean(USE_PERCENTILE, false) || getBoolean(USE_ZSCORE, false))) {
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
        if(getBoolean(USE_REAL_TIME_PERIOD, false) && getBoolean(USE_TUPLE_COUNT_PERIOD, false)) {
            throw new ConfigurationException(String.format("Can only select one of %s or %s",
                                                           USE_REAL_TIME_PERIOD,
                                                           USE_TUPLE_COUNT_PERIOD));
        }
        else if(!(getBoolean(USE_REAL_TIME_PERIOD, false) || getBoolean(USE_TUPLE_COUNT_PERIOD, false))) {
            throw new ConfigurationException(String.format("Must select one of %s or %s",
                                                           USE_REAL_TIME_PERIOD,
                                                           USE_TUPLE_COUNT_PERIOD));
        }
    }

    public void loadSystemProperties() {
        System.getProperties().stringPropertyNames()
                .stream()
                .filter(e -> e.startsWith("macrobase"))
                .forEach(e -> set(e, System.getProperty(e)));
    }
}
