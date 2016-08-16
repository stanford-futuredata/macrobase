package macrobase.conf;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.dropwizard.Configuration;
import macrobase.analysis.stats.*;
import macrobase.ingest.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
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

    public static final String RANDOM_PROJECTION_K = "macrobase.analysis.randomProjection.k";

    public static final String TRUNCATE_K = "macrobase.analysis.truncate.k";

    public static final String DATA_LOADER_TYPE = "macrobase.loader.loaderType";
    public static final String TIME_COLUMN = "macrobase.loader.timeColumn";
    public static final String ATTRIBUTES = "macrobase.loader.attributes";
    public static final String METRICS = "macrobase.loader.metrics";
    public static final String LOW_METRIC_TRANSFORM = "macrobase.analysis.metrics.lowTransform";

    public static final String JDBC_PROPERTIES = "macrobase.loader.jdbc.properties";

    public static final String BASE_QUERY = "macrobase.loader.db.baseQuery";
    public static final String DB_USER = "macrobase.loader.db.user";
    public static final String DB_PASSWORD = "macrobase.loader.db.password";
    public static final String DB_NAME = "macrobase.loader.db.database";
    public static final String DB_URL = "macrobase.loader.db.url";
    public static final String DB_CACHE_DIR = "macrobase.loader.db.cacheDirectory";
    public static final String DB_CACHE_CHUNK_SIZE = "macrobase.loader.db.cacheChunkSizeTuples";

    public static final String CSV_INPUT_FILE = "macrobase.loader.csv.file";
    public static final String CSV_COMPRESSION = "macrobase.loader.csv.compression";

    public static final String OUTLIER_STATIC_THRESHOLD = "macrobase.analysis.classify.outlierStaticThreshold";

    private final DatumEncoder datumEncoder;

    public MacroBaseConf() {
        datumEncoder = new DatumEncoder();
        _conf = new HashMap<>();
    }

    public DataIngester constructIngester() throws ConfigurationException, SQLException, IOException {
        DataIngesterType ingesterType = null;
        if (!_conf.containsKey(DATA_LOADER_TYPE)) {
            ingesterType = MacroBaseDefaults.DATA_LOADER_TYPE;
        }

        try {
            if(ingesterType == null) {
                ingesterType = DataIngesterType.valueOf(_conf.get(DATA_LOADER_TYPE));
            }
        } catch (IllegalArgumentException e) {
            try {
                Class c = Class.forName(_conf.get(DATA_LOADER_TYPE));
                Constructor<?> cons = c.getConstructor(MacroBaseConf.class);
                Object ao = cons.newInstance(this);

                if (!(ao instanceof DataIngester)) {
                    throw new ConfigurationException(String.format("%s is not an instance of DataIngester", ao.toString()));
                }

                return (DataIngester)ao;
            } catch (Exception e2) {
                log.error("an error occurred creating ingester", e2);
                throw new ConfigurationException(String.format("error instantiating ingester of type %s: %s", _conf.get(DATA_LOADER_TYPE), e2.getMessage()));
            }
        }

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
        MOVING_AVERAGE
    }

    public Random getRandom() {
        Long seed = getLong(RANDOM_SEED, null);
        if (seed != null) {
            return new Random(seed);
        } else {
            return new Random();
        }
    }

    public BatchTrainScore constructTransform()
            throws ConfigurationException {

        TransformType transformType = null;

        if(!_conf.containsKey(TRANSFORM_TYPE)) {
            transformType = MacroBaseDefaults.TRANSFORM_TYPE;
        }

        try {
            if(transformType == null) {
                transformType = TransformType.valueOf(_conf.get(TRANSFORM_TYPE));
            }
        } catch (IllegalArgumentException e) {
            try {
                Class c = Class.forName(_conf.get(TRANSFORM_TYPE));
                Constructor<?> cons = c.getConstructor(MacroBaseConf.class);
                Object ao = cons.newInstance(this);

                if (!(ao instanceof BatchTrainScore)) {
                    throw new ConfigurationException(String.format("%s is not an instance of BatchTrainScore", ao.toString()));
                }

                return (BatchTrainScore)ao;
            } catch (Exception e2) {
                log.error("an error occurred creating transform", e2);
                throw new ConfigurationException(String.format("error instantiating transform of type %s: %s", _conf.get(TRANSFORM_TYPE), e2.getMessage()));
            }
        }

        switch (transformType) {
            case MAD_OR_MCD:
                int metricsDimensions = this.getStringList(METRICS).size();
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
            default:
                throw new RuntimeException("Unhandled transform class!" + transformType);
        }
    }

    public enum DataIngesterType {
        CSV_LOADER,
        POSTGRES_LOADER,
        CACHING_POSTGRES_LOADER,
        MYSQL_LOADER,
        CACHING_MYSQL_LOADER
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
        return _conf.get(key).length() > 0 ? Arrays.asList(_conf.get(key).split(",[ ]*")).stream().map(
                Double::parseDouble).collect(Collectors.toList()) : new ArrayList<>();
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

    public void loadSystemProperties() {
        System.getProperties().stringPropertyNames()
                .stream()
                .filter(e -> e.startsWith("macrobase"))
                .forEach(e -> set(e, System.getProperty(e)));
    }
}
