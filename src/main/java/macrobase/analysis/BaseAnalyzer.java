package macrobase.analysis;

import macrobase.analysis.outlier.*;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseConf.DataLoaderType;
import macrobase.conf.MacroBaseConf.DetectorType;
import macrobase.conf.MacroBaseDefaults;
import macrobase.ingest.CsvLoader;
import macrobase.ingest.DataLoader;
import macrobase.ingest.DiskCachingPostgresLoader;
import macrobase.ingest.PostgresLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.List;

public class BaseAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(BaseAnalyzer.class);

    protected final Double zScore;
    protected final Double targetPercentile;
    protected final Double minSupport;
    protected final Double minOIRatio;
    protected final Long randomSeed;
    protected final DetectorType detectorType;
    protected final Boolean forceUsePercentile;
    protected final Boolean forceUseZScore;

    protected final Double mcdAlpha;
    protected final Double mcdStoppingDelta;

    protected final KDE.Bandwidth kdeBandwidth;
    protected final KDE.KernelType kdeKernelType;

    protected final Integer binnedKDEBins;

    protected final DataLoaderType dataLoaderType;
    protected final List<String> attributes;
    protected final List<String> lowMetrics;
    protected final List<String> highMetrics;

    protected final MacroBaseConf conf;

    protected final String queryName;

    protected final String storeAnalysisResults;

    public BaseAnalyzer(MacroBaseConf conf) throws ConfigurationException {
        this.conf = conf;

        queryName = conf.getString(MacroBaseConf.QUERY_NAME, MacroBaseDefaults.QUERY_NAME);
        log.debug("Running query {}", queryName);
        log.debug("CONFIG:\n{}", conf.toString());

        zScore = conf.getDouble(MacroBaseConf.ZSCORE, MacroBaseDefaults.ZSCORE);
        targetPercentile = conf.getDouble(MacroBaseConf.TARGET_PERCENTILE, MacroBaseDefaults.TARGET_PERCENTILE);
        minOIRatio = conf.getDouble(MacroBaseConf.MIN_OI_RATIO, MacroBaseDefaults.MIN_OI_RATIO);
        minSupport = conf.getDouble(MacroBaseConf.MIN_SUPPORT, MacroBaseDefaults.MIN_SUPPORT);
        randomSeed = conf.getLong(MacroBaseConf.RANDOM_SEED, MacroBaseDefaults.RANDOM_SEED);
        detectorType = conf.getDetectorType();
        forceUsePercentile = conf.getBoolean(MacroBaseConf.USE_PERCENTILE, MacroBaseDefaults.USE_PERCENTILE);
        forceUseZScore = conf.getBoolean(MacroBaseConf.USE_ZSCORE, MacroBaseDefaults.USE_ZSCORE);

        mcdAlpha = conf.getDouble(MacroBaseConf.MCD_ALPHA, MacroBaseDefaults.MCD_ALPHA);
        mcdStoppingDelta = conf.getDouble(MacroBaseConf.MCD_STOPPING_DELTA, MacroBaseDefaults.MCD_STOPPING_DELTA);

        kdeBandwidth = conf.getKDEBandwidth();
        kdeKernelType = conf.getKDEKernelType();

        binnedKDEBins = conf.getInt(MacroBaseConf.BINNED_KDE_BINS, MacroBaseDefaults.BINNED_KDE_BINS);

        dataLoaderType = conf.getDataLoaderType();
        attributes = conf.getStringList(MacroBaseConf.ATTRIBUTES);
        lowMetrics = conf.getStringList(MacroBaseConf.LOW_METRICS);
        highMetrics = conf.getStringList(MacroBaseConf.HIGH_METRICS);

        storeAnalysisResults = conf.getString(MacroBaseConf.STORE_ANALYSIS_RESULTS, MacroBaseDefaults.STORE_ANALYSIS_RESULTS);
    }

    public DataLoader constructLoader() throws ConfigurationException, SQLException, IOException {
        if (conf.getDataLoaderType() == DataLoaderType.CSV_LOADER) {
            return new CsvLoader(conf);
        } else if (conf.getDataLoaderType() == DataLoaderType.POSTGRES_LOADER) {
            return new PostgresLoader(conf);
        } else if (conf.getDataLoaderType() == DataLoaderType.CACHING_POSTGRES_LOADER) {
            return new DiskCachingPostgresLoader(conf);
        }

        throw new ConfigurationException(String.format("Unknown data loader type: %s", dataLoaderType));
    }

    protected OutlierDetector constructDetector(Long randomSeed) {
        int metricsDimensions = lowMetrics.size() + highMetrics.size();

        switch (detectorType) {
            case MAD_OR_MCD:
                if (metricsDimensions == 1) {
                    log.info("By default: using MAD detector for dimension 1 metric.");
                    return new MAD();
                } else {
                    log.info("By default: using MCD detector for dimension {} metrics.", metricsDimensions);
                    MinCovDet ret = new MinCovDet(metricsDimensions);
                    if (randomSeed != null) {
                        ret.seedRandom(randomSeed);
                    }
                    return ret;
                }
            case MAD:
                log.info("Using MAD detector.");
                return new MAD();
            case MCD:
                log.info("Using MCD detector.");
                MinCovDet ret = new MinCovDet(metricsDimensions);
                if (randomSeed != null) {
                    ret.seedRandom(randomSeed);
                }
                return ret;
            case ZSCORE:
                log.info("Using ZScore detector.");
                return new ZScore();
            case KDE:
                log.info("Using KDE detector.");
                return new KDE(kdeKernelType, kdeBandwidth);
            case BINNED_KDE:
                log.info("Using BinnedKDE detector.");
                return new BinnedKDE(kdeKernelType, kdeBandwidth, binnedKDEBins);
            default:
                throw new RuntimeException("Unhandled detector class!" + detectorType);
        }
    }

}
