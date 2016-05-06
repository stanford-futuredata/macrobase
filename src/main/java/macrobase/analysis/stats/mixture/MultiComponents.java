package macrobase.analysis.stats.mixture;

import org.apache.commons.math3.special.Gamma;

public class MultiComponents implements MixingComponents {

    private double priorAlpha;
    private double[] coeffs;
    private int K;

    public MultiComponents(double prior, int clusters) {
        K = clusters;
        priorAlpha = prior;
        coeffs = new double[clusters];
        for (int i = 0; i < K; i++) {
            coeffs[i] = 1. / K;
        }
    }

    @Override
    public double[] calcExpectationLog() {
        double[] exLogMixing = new double[K];
        double sum = 0;
        for (double coeff : coeffs) {
            sum += coeff;
        }
        for (int i = 0; i < K; i++) {
            exLogMixing[i] = Gamma.digamma(coeffs[i]) - Gamma.digamma(sum);
        }
        return exLogMixing;
    }

    @Override
    public void update(double[][] r) {
        double[] clusterWeight = MeanFieldGMM.calculateClusterWeights(r);
        for (int k = 0; k < K; k++) {
            coeffs[k] = priorAlpha + clusterWeight[k];
        }
    }

    public void moveNatural(double[][] r, double pace, double portion) {
        double[] clusterWeight = MeanFieldGMM.calculateClusterWeights(r);
        for (int k = 0; k < K; k++) {
            coeffs[k] = StochVarInfGMM.step(coeffs[k], priorAlpha + portion * clusterWeight[k], pace);
        }
    }

    public double[] getCoeffs() {
        return coeffs;
    }

    public double getPrior() {
        return priorAlpha;
    }
}
