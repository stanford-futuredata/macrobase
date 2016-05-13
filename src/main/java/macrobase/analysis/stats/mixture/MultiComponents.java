package macrobase.analysis.stats.mixture;

import org.apache.commons.math3.special.Gamma;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiComponents implements MixingComponents {
    private static final Logger log = LoggerFactory.getLogger(MultiComponents.class);

    private double priorAlpha;
    private double[] coeffs;
    private int K;

    // Auxiliaries.
    private double sumCoeffs;

    public MultiComponents(double prior, int clusters) {
        K = clusters;
        priorAlpha = prior;
        coeffs = new double[clusters];
        sumCoeffs = 0;
        for (int i = 0; i < K; i++) {
            coeffs[i] = 1. / K;
            sumCoeffs += coeffs[i];
        }
    }

    @Override
    public double[] calcExpectationLog() {
        double[] exLogMixing = new double[K];
        for (int i = 0; i < K; i++) {
            exLogMixing[i] = Gamma.digamma(coeffs[i]) - Gamma.digamma(sumCoeffs);
        }
        return exLogMixing;
    }

    @Override
    public void update(double[][] r) {
        double[] clusterWeight = VariationalInference.calculateClusterWeights(r);
        sumCoeffs = 0;
        for (int k = 0; k < K; k++) {
            coeffs[k] = priorAlpha + clusterWeight[k];
            sumCoeffs += coeffs[k];
        }
    }

    public void moveNatural(double[][] r, double pace, double portion) {
        double[] clusterWeight = VariationalInference.calculateClusterWeights(r);
        sumCoeffs = 0;
        for (int k = 0; k < K; k++) {
            coeffs[k] = VariationalInference.step(coeffs[k], priorAlpha + portion * clusterWeight[k], pace);
            sumCoeffs += coeffs[k];
        }
    }

    @Override
    public double[] getNormalizedClusterProportions() {
        double[] normalized = new double[coeffs.length];
        for (int i=0; i< coeffs.length; i++) {
            normalized[i] = coeffs[i] / sumCoeffs;
        }
        return normalized;
    }

    public double[] getCoeffs() {
        return coeffs;
    }

    public double getPrior() {
        return priorAlpha;
    }
}
