package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.MultivariateTDistribution;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.special.Gamma;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

/**
 * Fit Gaussian Mixture models using Variational Bayes
 */
public class FiniteGMM extends MeanFieldGMM {
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
        initConstants(data);
        initializeBaseNormalWishart(data);
        initializeBaseMixing();
        initializeSticks();
        initializeAtoms(data);

        log.debug("components");
        mixingComponents = new MultiComponents(priorAlpha, K);
        mixingComponents.initalize();
        clusters = new NormalWishartClusters(atomLoc, atomBeta, atomOmega, atomDOF);
        clusters.initializeBase(baseLoc, baseBeta, baseOmega, baseNu);

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
            log.debug("inte: {}", iter);
            // Step 1. update local variables
            log.debug("0.");
            exLnMixingContribution = mixingComponents.calcExpectationLog();
            log.debug("1. {}", exLnMixingContribution);
            lnPrecision = clusters.calculateExLogPrecision();
            log.debug("2. {}", lnPrecision);
            dataLogLike = clusters.calcLogLikelyFixedPrec(data);
            log.debug("3. {}", dataLogLike);
            r = normalizeLogProbas(exLnMixingContribution, lnPrecision, dataLogLike);

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



    @Override
    protected void initializeBaseNormalWishart(List<Datum> data) {
        D = data.get(0).getMetrics().getDimension();
        baseNu = 0.1;
        baseBeta = 0.1;
        baseLoc = new ArrayRealVector(D);
        baseOmega = MatrixUtils.createRealIdentityMatrix(D);
        baseOmegaInverse = AlgebraUtils.invertMatrix(baseOmega);
    }

    @Override
    protected void initializeBaseMixing() {
        priorAlpha = 0.1;
    }

    @Override
    protected void initializeSticks() {
        mixingCoeffs = new double[K];
        for (int k = 0; k < this.K; k++) {
            mixingCoeffs[k] = 1. / K;
        }
    }

    @Override
    protected void initializeAtoms(List<Datum> data) {
        atomBeta = new double[K];
        atomDOF = new double[K];
        atomOmega = new ArrayList<>(K);

        // Initialize
        if (initialClusterCentersFile != null) {
            try {
                atomLoc = initalizeClustersFromFile(initialClusterCentersFile, K);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                atomLoc = gonzalezInitializeMixtureCenters(data, K);
            }
        } else {
            atomLoc = gonzalezInitializeMixtureCenters(data, K);
        }
        log.debug("initialized cluster centers as: {}", atomLoc);
        for (int k = 0; k < this.K; k++) {
            atomBeta[k] = baseBeta;
            atomDOF[k] = baseNu;
            atomOmega.add(baseOmega);
        }
        log.debug("atomOmega: {}", atomOmega);
    }

    private void updatePredictiveDistributions(MultiComponents mixingComonents, NormalWishartClusters clusters) {
        predictiveDistributions = clusters.constructPredictiveDistributions();
        mixingCoeffs = mixingComonents.getCoeffs();
    }

    @Override
    protected void updatePredictiveDistributions() {
        predictiveDistributions = new ArrayList<>(K);
        for (int k = 0; k < this.K; k++) {
            double scale = (atomDOF[k] + 1 - D) * atomBeta[k] / (1 + atomBeta[k]);
            RealMatrix ll = AlgebraUtils.invertMatrix(atomOmega.get(k).scalarMultiply(scale));
            // TODO: MultivariateTDistribution should support real values for 3rd parameters
            predictiveDistributions.add(new MultivariateTDistribution(atomLoc.get(k), ll, (int) (atomDOF[k] - 1 - D)));
        }
    }

    /**
     * Make sure to update clusterWeight before!!!!
     * @param r
     */
    @Override
    protected void updateSticks(double[][] r) {
        double[] clusterWeight = calculateClusterWeights(r);
        for (int k=0; k<K; k++) {
            mixingCoeffs[k] = priorAlpha + clusterWeight[k];
        }
    }

    @Override
    protected double[] calcExQlogMixing() {
        int num = mixingCoeffs.length;
        double[] exLogMixing = new double[num];
        double sum = 0;
        for (double coeff : mixingCoeffs) {
            sum += coeff;
        }
        for (int i = 0; i < num; i++) {
            exLogMixing[i] = Gamma.digamma(mixingCoeffs[i]) - Gamma.digamma(sum);
        }
        return exLogMixing;
    }

    @Override
    public double score(Datum datum) {
        double density = 0;
        double sum_alpha = 0;
        for (int k = 0; k < this.K; k++) {
            // If the mixture is very improbable, skip.
            if (Math.abs(mixingCoeffs[k] - priorAlpha) < 1e-4) {
                continue;
            }
            sum_alpha += mixingCoeffs[k];
            density += mixingCoeffs[k] * this.predictiveDistributions.get(k).density(datum.getMetrics());
        }
        return Math.log(density / sum_alpha);
    }

    @Override
    public double[] getClusterProportions() {
        return mixingCoeffs;
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
