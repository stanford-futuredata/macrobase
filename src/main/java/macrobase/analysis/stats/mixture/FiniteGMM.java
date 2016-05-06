package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.MultivariateTDistribution;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Fit Gaussian Mixture models using Variational Bayes
 */
public class FiniteGMM extends BatchMixtureModel{
    private static final Logger log = LoggerFactory.getLogger(FiniteGMM.class);
    private final String initialClusterCentersFile;

    private int K;  // Number of mixture components

    private double priorAlpha;

    private double[] mixingCoeffs;
    private List<MultivariateTDistribution> predictiveDistributions;

    // Components.
    private MultiComponents mixingComponents;
    private NormalWishartClusters clusters;

    public FiniteGMM(MacroBaseConf conf) {
        super(conf);
        this.K = conf.getInt(MacroBaseConf.NUM_MIXTURES, MacroBaseDefaults.NUM_MIXTURES);
        log.debug("created Gaussian MM with {} mixtures", this.K);
        this.initialClusterCentersFile = conf.getString(MacroBaseConf.MIXTURE_CENTERS_FILE, null);
    }


    // @Override
   // public void train(List<Datum> data) {
   //     super.train(data, K);
   // }
    @Override
    public void train(List<Datum> data) {
        // 0. Initialize all approximating factors
        log.debug("training locally");
        mixingComponents = new MultiComponents(priorAlpha, K);
        mixingComponents.initialize();
        clusters = new NormalWishartClusters(K, data.get(0).getMetrics().getDimension());
        clusters.initializeBaseForFinite(data);
        clusters.initalizeAtomsForFinite(data, initialClusterCentersFile, conf.getRandom());
        //clusters.initializeBase(baseLoc, baseBeta, baseOmega, baseNu);

        log.debug("actual training");
        train(data, mixingComponents, clusters, maxIterationsToConverge, progressCutoff);
    }

    public void train(List<Datum> data, MultiComponents mixingComponents, NormalWishartClusters clusters, int maxIter, double improvementCutoff) {
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
            logLikelihood = calculateLogLikelihood(data, mixingComponents, clusters);
            if (checkTermination(logLikelihood, oldLogLikelihood, iter, maxIter, improvementCutoff)) {
                break;
            }
        }
    }

    private boolean checkTermination(double logLikelihood, double oldLogLikelihood, int iteration, int maxIter, double improvementCutoff) {
        log.debug("log likelihood after iteration {} is {}", iteration, logLikelihood);

        if (iteration >= maxIter) {
            log.debug("Breaking because have already run {} iterations", iteration);
            return true;
        }

        double improvement = (logLikelihood - oldLogLikelihood) / (-logLikelihood);
        if (improvement >= 0 && improvement < improvementCutoff) {
            log.debug("Breaking because improvement was {} percent", improvement * 100);
            return true;
        } else {
            log.debug("improvement is : {}%", improvement * 100);
        }
        log.debug(".........................................");
        return false;
    }

    private double calculateLogLikelihood(List<Datum> data, MultiComponents mixingComonents, NormalWishartClusters clusters) {
        updatePredictiveDistributions(mixingComonents, clusters);
        double logLikelihood = 0;
        for (Datum d : data) {
            logLikelihood += score(d);
        }
        return logLikelihood;
    }

    private void updatePredictiveDistributions(MultiComponents mixingComonents, NormalWishartClusters clusters) {
        predictiveDistributions = clusters.constructPredictiveDistributions();
        mixingCoeffs = mixingComonents.getCoeffs();
    }

    @Override
    public double score(Datum datum) {
        double density = 0;
        double sum_alpha = 0;
        double[] mixingCoeffs = mixingComponents.getCoeffs();
        double prior = mixingComponents.getPrior();
        for (int k = 0; k < this.K; k++) {
            // If the mixture is very improbable, skip.
            if (Math.abs(mixingCoeffs[k] - prior) < 1e-4) {
                continue;
            }
            sum_alpha += mixingCoeffs[k];
            density += mixingCoeffs[k] * this.predictiveDistributions.get(k).density(datum.getMetrics());
        }
        return Math.log(density / sum_alpha);
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        throw new NotImplementedException("");
    }

    @Override
    public double[] getClusterProportions() {
        return mixingComponents.getCoeffs();
    }

    @Override
    public List<RealMatrix> getClusterCovariances() {
        return clusters.getMAPCovariances();
    }

    @Override
    public List<RealVector> getClusterCenters() {
        return clusters.getMAPLocations();
    }

    public double[] getPriorAdjustedClusterProportions() {
        double[] mixingCoeffs = mixingComponents.getCoeffs();
        double sum = - priorAlpha; // to adjust for prior.
        for (double coeff : mixingCoeffs) {
            sum += coeff;
        }
        double[] proportions = new double[K];
        for (int i=0; i<K; i++) {
            proportions[i] = (mixingCoeffs[i] - priorAlpha / K) / sum;
        }
        return proportions;
    }

    @Override
    public double[] getClusterProbabilities(Datum d) {
        double[] probas = new double[K];
        double[] weights = getClusterProportions();
        double normalizingConstant = 0;
        for (int i = 0; i < K; i++) {
            probas[i] = weights[i] * predictiveDistributions.get(i).density(d.getMetrics());
            normalizingConstant += probas[i];
        }
        for (int i = 0; i < K; i++) {
            probas[i] /= normalizingConstant;
        }
        return probas;
    }
}
