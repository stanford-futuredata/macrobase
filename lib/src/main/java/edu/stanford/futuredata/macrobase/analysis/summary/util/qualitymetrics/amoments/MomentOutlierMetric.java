package edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.amoments;

import edu.stanford.futuredata.macrobase.analysis.summary.util.qualitymetrics.QualityMetric;
import msolver.PMomentSolverBuilder;
import msolver.struct.ArcSinhMomentStruct;
import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Quality metric used in the power cube pipeline. Uses min, max and moments.
 */
public abstract class MomentOutlierMetric implements QualityMetric {
    private Logger log = LoggerFactory.getLogger("MomentOutlierMetric");
    int k;
    private int minIdx;
    private int maxIdx;
    int powerSumsBaseIdx;
    int outlierCountIdx;

    double quantile;  // eg, 0.99
    double cutoff;
    double globalOutlierCount;

    private int[] callTypeCount = new int[4];

    private double tolerance = 1e-9;
    private boolean useCascade = true;

    MomentOutlierMetric(double quantile, int k) {
        this.quantile = quantile;
        this.k = k;
    }

    protected PMomentSolverBuilder getBuilderFromAggregates(double[] aggregates) {
        PMomentSolverBuilder b = new PMomentSolverBuilder(momentDataFromAggregates(aggregates));
        b.initialize();
        return b;
    }
    private ArcSinhMomentStruct momentDataFromAggregates(double[] aggregates) {
        double amin = FastMath.asinh(aggregates[minIdx]);
        double amax = FastMath.asinh(aggregates[maxIdx]);
        double[] powerSums = Arrays.copyOfRange(
                aggregates,
                powerSumsBaseIdx,
                powerSumsBaseIdx+k
        );

        ArcSinhMomentStruct m = new ArcSinhMomentStruct(
                amin,
                amax,
                powerSums
        );
        return m;
    }
    @Override
    public QualityMetric initialize(double[] globalAggregates) {
        globalOutlierCount = globalAggregates[powerSumsBaseIdx] * (1.0-quantile);
        ArcSinhMomentStruct ms = momentDataFromAggregates(globalAggregates);
        PMomentSolverBuilder builder = new PMomentSolverBuilder(ms);
        try {
            cutoff = builder.getQuantile(quantile);
            log.info("Outlier Cutoff: "+cutoff);
        } catch (Exception e) {
            cutoff = quantile * (globalAggregates[maxIdx] - globalAggregates[minIdx]) + globalAggregates[minIdx];
        }
        return this;
    }

    abstract double getOutlierRateNeeded(double[] aggregates, double threshold);

    private Action actionIfBelowThreshold() {
        if (isMonotonic()) {
            return Action.PRUNE;
        } else {
            return Action.NEXT;
        }
    }

    @Override
    public Action getAction(double[] aggregates, double threshold) {
        double outlierRateNeeded = getOutlierRateNeeded(aggregates, threshold);
        PMomentSolverBuilder builder = getBuilderFromAggregates(aggregates);
        boolean aboveThreshold = builder.checkThreshold(cutoff, outlierRateNeeded);
        callTypeCount[builder.getCallType()]++;
        if (aboveThreshold) {
            return Action.KEEP;
        } else {
            return actionIfBelowThreshold();
        }
    }

    public int[] getCallTypeCount() {
        return callTypeCount;
    }

    public void setUseCascade(boolean useCascade) { this.useCascade = useCascade; }
    public void setTolerance(double tolerance) { this.tolerance = tolerance; }
    public double getCutoff() { return cutoff;}
    
    public void setStandardIndices(int minIdx, int maxIdx, int powerSumsBaseIdx) {
        this.minIdx = minIdx;
        this.maxIdx = maxIdx;
        this.powerSumsBaseIdx = powerSumsBaseIdx;
    }
    public void setOutlierCountIdx(int outlierCountIdx) {
        this.outlierCountIdx = outlierCountIdx;
    }
}
