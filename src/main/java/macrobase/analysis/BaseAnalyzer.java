package macrobase.analysis;

import macrobase.analysis.outlier.*;
import macrobase.runtime.standalone.BaseStandaloneConfiguration;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
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
    
    protected double samplingRate = 1.0;

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
    
    public void setSamplingRate(double samplingRate) {
    	this.samplingRate = samplingRate;
    }

    protected OutlierDetector constructDetector(int metricsDimensions) {
        if(detectorType == null) {
            if (metricsDimensions == 1) {
                log.info("By default: using MAD detector for dimension 1 metric.");
                return new MAD();
            } else {
                log.info("By default: using MCD detector for dimension {} metrics.", metricsDimensions);
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
            } else if(detectorType == BaseStandaloneConfiguration.DetectorType.KDE) {
            	log.info("Using KDE detector.");
                double matrix_scale = 0.1;  // Scale identity matrix by this much
                RealMatrix bandwidth = MatrixUtils.createRealIdentityMatrix(metricsDimensions);
                bandwidth = bandwidth.scalarMultiply(matrix_scale);
                return new KDE(KDE.Kernel.EPANECHNIKOV_MULTIPLICATIVE, bandwidth);
            } else {
                throw new RuntimeException("Unhandled detector class!"+ detectorType);
            }
        }
    }
}
