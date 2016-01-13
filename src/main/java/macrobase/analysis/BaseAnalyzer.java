package macrobase.analysis;

abstract public class BaseAnalyzer {
    protected double ZSCORE = 3;
    protected double TARGET_PERCENTILE = 0.01;
    protected double MIN_SUPPORT = 0.001;
    protected double MIN_INLIER_RATIO = 1;

    protected boolean forceUsePercentile = true;
    protected boolean forceUseZScore = false;
    
    protected double alphaMCD = 0.5;
    protected double stoppingDeltaMCD = 1e-3;

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
}
