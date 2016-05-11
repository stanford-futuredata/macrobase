package macrobase.analysis.stats.mixture;

import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class VariationalInference {
    private static final Logger log = LoggerFactory.getLogger(VariationalInference.class);

    public static double[][] normalizeLogProbabilities(double[] lnMixing, double[] lnPrecision, double[][] dataLogLike) {
        double[][] r = new double[dataLogLike.length][lnMixing.length];
        for (int n = 0; n < dataLogLike.length; n++) {
            double normalizingConstant = 0;
            for (int k = 0; k < lnMixing.length; k++) {
                r[n][k] = Math.exp(lnMixing[k] + lnPrecision[k] + dataLogLike[n][k]);
                normalizingConstant += r[n][k];
            }
            for (int k = 0; k < lnMixing.length; k++) {
                if (normalizingConstant > 0) {
                    r[n][k] /= normalizingConstant;
                }
            }
        }
        return r;
    }

    public static void trainStochastic(VarGMM model, List<Datum> data, MixingComponents mixingComponents, NormalWishartClusters clusters, int desiredMinibatchSize, double delay, double forgettingRate) {
        double[] exLnMixingContribution;
        double[] lnPrecision;
        double[][] dataLogLike;
        double[][] r;
        int minibatchSize;
        List<Datum> miniBatch;

        final int N = data.size();
        final int partitions = N / Math.min(data.size(), desiredMinibatchSize);

        double logLikelihood = -Double.MAX_VALUE;
        for (int iter = 1; ; iter++) {
            double pace = Math.pow(iter + delay, -forgettingRate);
            log.debug("pace = {}", pace);

            for (int p = 0; p < partitions; p++) {
                // Step 0. Create the minibatch.
                miniBatch = new ArrayList<>(desiredMinibatchSize);
                for (int i = 0; i < N; i += partitions) {
                    miniBatch.add(data.get(i));
                }

                minibatchSize = miniBatch.size();

                log.debug("minibatch Size = {}", minibatchSize);

                // Step 1. Update local variables
                exLnMixingContribution = mixingComponents.calcExpectationLog();
                lnPrecision = clusters.calculateExLogPrecision();
                dataLogLike = clusters.calcLogLikelyFixedPrec(miniBatch);
                r = VariationalInference.normalizeLogProbabilities(exLnMixingContribution, lnPrecision, dataLogLike);

                // Step 2. Update global variables
                mixingComponents.moveNatural(r, pace, 1. * N /minibatchSize);
                clusters.moveNatural(miniBatch, r, pace, 1. * N / minibatchSize);

                double oldLogLikelihood = logLikelihood;
                logLikelihood = model.calculateLogLikelihood(data, mixingComponents, clusters);
                if (model.checkTermination(logLikelihood, oldLogLikelihood, iter)) {
                    return;
                }
            }
        }
    }

    public static void trainMeanField(VarGMM model, List<Datum> data, MixingComponents mixingComponents, NormalWishartClusters clusters) {
        log.debug("inside main trainMeanField");
        double[] exLnMixingContribution;
        double[] lnPrecision;
        double[][] dataLogLike;
        double[][] r;

        int N = data.size();
        double logLikelihood = -Double.MAX_VALUE;
        for (int iter = 1; ; iter++) {
            // Step 1. update local variables
            exLnMixingContribution = mixingComponents.calcExpectationLog();
            lnPrecision = clusters.calculateExLogPrecision();
            dataLogLike = clusters.calcLogLikelyFixedPrec(data);
            r = VariationalInference.normalizeLogProbabilities(exLnMixingContribution, lnPrecision, dataLogLike);

            // Step 2. update global variables
            mixingComponents.update(r);
            clusters.update(data, r);

            double oldLogLikelihood = logLikelihood;
            logLikelihood = model.calculateLogLikelihood(data, mixingComponents, clusters);
            if (model.checkTermination(logLikelihood, oldLogLikelihood, iter)) {
                break;
            }
        }
    }

    public static double step(double value, double newValue, double pace) {
        return (1 - pace) * value + pace * newValue;
    }

    public static RealVector step(RealVector start, RealVector end, double pace) {
        return start.mapMultiply(1 - pace).add(end.mapMultiply(pace));
    }

    public static RealMatrix step(RealMatrix start, RealMatrix end, double pace) {
        return start.scalarMultiply(1 - pace).add(end.scalarMultiply(pace));
    }

    protected static double[] calculateClusterWeights(double[][] r) {
        int N = r.length;
        int K = r[0].length;
        double[] clusterWeight = new double[K];
        for (int k = 0; k < K; k++) {
            for (int n = 0; n < N; n++) {
                clusterWeight[k] += r[n][k];
            }
        }
        return clusterWeight;
    }
}
