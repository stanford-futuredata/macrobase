package macrobase.analysis.stats.mixture;

import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MeanFieldVariationalInference {
    private static final Logger log = LoggerFactory.getLogger(MeanFieldVariationalInference.class);

    public static double[][] normalizeLogProbas(double[] lnMixing, double[] lnPrecision, double[][] dataLogLike) {
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

    public static void train(VarGMM model, List<Datum> data, MixingComponents mixingComponents, NormalWishartClusters clusters) {
        log.debug("inside main train");
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
            r = MeanFieldVariationalInference.normalizeLogProbas(exLnMixingContribution, lnPrecision, dataLogLike);

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

}
