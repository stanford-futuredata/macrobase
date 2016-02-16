package macrobase.analysis;

import com.codahale.metrics.MetricRegistryListener;
import macrobase.analysis.outlier.*;
import macrobase.runtime.standalone.BaseStandaloneConfiguration;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class BaseAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(BaseAnalyzer.class);

    protected double ZSCORE = 3;
    protected double TARGET_PERCENTILE = 0.99;
    protected double MIN_SUPPORT = 0.001;
    protected double MIN_INLIER_RATIO = 1;

    protected Boolean seedRand = false;

    protected BaseStandaloneConfiguration.DetectorType detectorType;

    protected boolean forceUsePercentile = true;
    protected boolean forceUseZScore = false;
    
    protected double alphaMCD = 0.5;
    protected double stoppingDeltaMCD = 1e-3;

    protected BaseStandaloneConfiguration serverConfiguration;

    public void setSeedRand(boolean doSeed) { seedRand = true; }

    public BaseAnalyzer() {
        serverConfiguration = null;
    }

    public BaseAnalyzer(BaseStandaloneConfiguration configuration) {
        serverConfiguration = configuration;
    }

    public void setDetectorType(BaseStandaloneConfiguration.DetectorType detectorType) { this.detectorType = detectorType; }

    public void forceUsePercentile(boolean force) {
        forceUsePercentile = force;
    }

    public void forceUseZScore(boolean force) {
        forceUseZScore = force;
    }

    public void setZScore(double zscore) {
        ZSCORE = zscore;
    }

    public void setTargetPercentile(double targetPercentile) {
        TARGET_PERCENTILE = targetPercentile;
    }

    public void setMinSupport(double minSupport) {
        MIN_SUPPORT = minSupport;
    }

    public void setMinInlierRatio(double minInlierRatio) {
        MIN_INLIER_RATIO = minInlierRatio;
    }
    
    public void setAlphaMCD(double alphaMCD) {
    	this.alphaMCD = alphaMCD;
    }
    
    public void setStoppingDeltaMCD(double stoppingDeltaMCD) {
    	this.stoppingDeltaMCD = stoppingDeltaMCD;
    }

    protected OutlierDetector constructDetector(int metricsDimensions, boolean seedRand) {
        if(detectorType == null) {
            if (metricsDimensions == 1) {
                log.info("By default: using MAD detector for dimension 1 metric.");
                return new MAD();
            } else {
                log.info("By default: using MCD detector for dimension {} metrics.", metricsDimensions);
                MinCovDet ret = new MinCovDet(metricsDimensions);
                if(seedRand) {
                    ret.seedRandom(0);
                }
                return ret;
            }
        } else {
            log.debug("{}", detectorType);
            if(detectorType == BaseStandaloneConfiguration.DetectorType.MAD) {
                log.info("Using MAD detector.");
                return new MAD();
            } else if(detectorType == BaseStandaloneConfiguration.DetectorType.MCD) {
                log.info("Using MCD detector.");
                MinCovDet ret = new MinCovDet(metricsDimensions);
                if(seedRand) {
                    ret.seedRandom(0);
                }
                return ret;
            } else if(detectorType == BaseStandaloneConfiguration.DetectorType.ZSCORE) {
                log.info("Using ZScore detector.");
                return new ZScore();
            } else if(detectorType == BaseStandaloneConfiguration.DetectorType.KDE) {
            	log.info("Using KDE detector.");
                KDE.Bandwidth bandwidthType = serverConfiguration.getKernelBandwidth();
                if (bandwidthType != null) {
                    return new KDE(KDE.KernelType.EPANECHNIKOV_MULTIPLICATIVE, bandwidthType);
                }
                return new KDE(KDE.KernelType.EPANECHNIKOV_MULTIPLICATIVE, KDE.Bandwidth.OVERSHMOOTHED);
            } else if(detectorType == BaseStandaloneConfiguration.DetectorType.BINNED_KDE) {
                log.info("Using BinnedKDE detector.");
                KDE.Bandwidth bandwidthType = serverConfiguration.getKernelBandwidth();
                if (bandwidthType != null) {
                    return new BinnedKDE(KDE.KernelType.EPANECHNIKOV_MULTIPLICATIVE, bandwidthType);
                }
                return new BinnedKDE(KDE.KernelType.EPANECHNIKOV_MULTIPLICATIVE, KDE.Bandwidth.OVERSHMOOTHED);
            } else {
                throw new RuntimeException("Unhandled detector class!"+ detectorType);
            }
        }
    }
}
