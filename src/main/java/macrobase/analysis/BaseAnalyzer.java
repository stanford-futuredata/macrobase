package macrobase.analysis;

import macrobase.analysis.outlier.MAD;
import macrobase.analysis.outlier.MinCovDet;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.outlier.ZScore;
import macrobase.runtime.standalone.BaseStandaloneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class BaseAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(BaseAnalyzer.class);

    protected double ZSCORE = 3;
    protected double TARGET_PERCENTILE = 0.01;
    protected double MIN_SUPPORT = 0.001;
    protected double MIN_INLIER_RATIO = 1;

    protected BaseStandaloneConfiguration.DetectorType detectorType;

    protected boolean forceUsePercentile = true;
    protected boolean forceUseZScore = false;
    
    protected double alphaMCD = 0.5;
    protected double stoppingDeltaMCD = 1e-3;

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

    protected OutlierDetector constructDetector(int metricsDimensions) {
        if(detectorType == null) {
            if (metricsDimensions == 1) {
                log.info("By default: using MAD detector for dimension 1 metric.");
                detectorType = BaseStandaloneConfiguration.DetectorType.MAD;
                return new MAD();
            } else {
                log.info("By default: using MCD detector for dimension {} metrics.", metricsDimensions);
                detectorType = BaseStandaloneConfiguration.DetectorType.MCD;
                return new MinCovDet(metricsDimensions);
            }
        } else {
            if(detectorType == BaseStandaloneConfiguration.DetectorType.MAD) {
                log.info("Using MAD detector.");
                return new MAD();
            } else if(detectorType == BaseStandaloneConfiguration.DetectorType.MCD) {
                log.info("Using MCD detector.");
                return new MinCovDet(metricsDimensions);
            } else if(detectorType == BaseStandaloneConfiguration.DetectorType.ZSCORE) {
                log.info("Using ZScore detector.");
                return new ZScore();
            } else {
                throw new RuntimeException("Unhandled detector class!"+ detectorType);
            }
        }
    }
}
