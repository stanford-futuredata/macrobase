package macrobase.analysis;

import macrobase.analysis.outlier.*;
import macrobase.runtime.standalone.BaseStandaloneConfiguration;
import org.apache.commons.math3.linear.BlockRealMatrix;
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
            if (metricsDimensions < 100) {
                log.info("Using KDE detector.");
                BlockRealMatrix bandwidth = new BlockRealMatrix(metricsDimensions, metricsDimensions);
                for (int row = 0; row < metricsDimensions; row++) {
                    for (int column = 0; column < row; column++) {
                        bandwidth.setEntry(row, column, 0);
                        bandwidth.setEntry(column, row, 0);
                    }
                    bandwidth.setEntry(row, row, 1);
                }
                return new KDE(KDE.Kernel.EPANECHNIKOV_MULTIPLICATIVE, bandwidth);
            }

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
            } else {
                throw new RuntimeException("Unhandled detector class!"+ detectorType);
            }
        }
    }
}
