package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.MultivariateTDistribution;
import macrobase.analysis.stats.distribution.Wishart;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.*;
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

    private double[] clusterWeight;  // N_k (Bishop)

    // Useful constants for each dataset.


    public FiniteGMM(MacroBaseConf conf) {
        super(conf);
        this.K = conf.getInt(MacroBaseConf.NUM_MIXTURES, MacroBaseDefaults.NUM_MIXTURES);
        log.debug("created Gaussian MM with {} mixtures", this.K);
        this.initialClusterCentersFile = conf.getString(MacroBaseConf.MIXTURE_CENTERS_FILE, null);
    }


    // Custom initialization for FiniteGMM to keep the exact same behavior as before refactor.
    @Override
    protected void initializeBaseNormalWishart(List<Datum> data) {
        D = data.get(0).getMetrics().getDimension();
        baseNu = 0.1;
        baseBeta = 0.1;
        baseLoc = new ArrayRealVector(D);
        baseOmega = MatrixUtils.createRealIdentityMatrix(D);
        baseOmegaInverse = AlgebraUtils.invertMatrix(baseOmega);
    }

    private void initalizeBaseMixing() {
        priorAlpha = 0.1;
    }

    private void initializeSticks() {
        mixingCoeffs = new double[K];
        for (int k = 0; k < this.K; k++) {
            mixingCoeffs[k] = 1. / K;
        }
    }

    private void initalizeAtoms(List<Datum> data) {
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

    @Override
    public void train(List<Datum> data) {
        initConstants(data);
        initializeBaseNormalWishart(data);
        initalizeBaseMixing();
        initializeSticks();
        initalizeAtoms(data);

        int N = data.size();
        List<Wishart> wisharts;
        // density of each point with respect to each mixture component.
        double[][] r = new double[N][K];

        double logLikelihood = -Double.MAX_VALUE;
        for (int iteration = 0; ; iteration++) {

            // Useful to keep everything tidy.
            wisharts = constructWisharts(atomOmega, atomDOF);

            // 1. calculate expectation of densities of each point coming from individual clusters - r[n][k]

            // Calculate mixing coefficient log expectation
            double[] ex_log_mixing = calcExQlogMixing(mixingCoeffs);

            double[][] dataLogLike = calcLogLikelihoodFixedAtoms(data, atomLoc, atomBeta, atomOmega, atomDOF);

            for (int n = 0; n < N; n++) {
                double normalizingConstant = 0;
                for (int k = 0; k < this.K; k++) {
                    r[n][k] = Math.exp(ex_log_mixing[k] + 0.5 * wisharts.get(k).getExpectationLogDeterminantLambda() + dataLogLike[n][k]);
                    normalizingConstant += r[n][k];
                }
                for (int k = 0; k < this.K; k++) {
                    if (normalizingConstant == 0) {
                        continue;
                    }
                    r[n][k] /= normalizingConstant;
                }
            }

            // 2. Reevaluate clusters based on densities that we have for each point.
            updateSticks(r);
            updateAtoms(r, data);

            clusterWeight = calculateClusterWeights(r);

            predictiveDistributions = new ArrayList<>(K);
            for (int k = 0; k < this.K; k++) {
                double scale = (atomDOF[k] + 1 - D) * atomBeta[k] / (1 + atomBeta[k]);
                RealMatrix ll = AlgebraUtils.invertMatrix(atomOmega.get(k).scalarMultiply(scale));
                // TODO: MultivariateTDistribution should support real values for 3rd parameters
                predictiveDistributions.add(new MultivariateTDistribution(atomLoc.get(k), ll, (int) (atomDOF[k] - 1 - D)));
            }

            log.debug("cluster weights are at {}", clusterWeight);
            log.debug("cluster covariances are at {}", getClusterCovariances());

            double oldLogLikelihood = logLikelihood;
            logLikelihood = 0;
            for (int n = 0; n < N; n++) {
                logLikelihood += score(data.get(n));
            }

            log.debug("log likelihood after iteration {} is {}", iteration, logLikelihood);

            double improvement = (logLikelihood - oldLogLikelihood) / (-logLikelihood);
            if (improvement >= 0 && improvement < this.progressCutoff) {
                log.debug("Breaking because improvement was {} percent", improvement * 100);
                break;
            } else {
                log.debug("improvement is : {}%", improvement * 100);
            }
        }
    }

    /**
     * Make sure to update clusterWeight before!!!!
     * @param r
     */
    private void updateSticks(double[][] r) {
        double[] clusterWeight = calculateClusterWeights(r);
        for (int k=0; k<K; k++) {
            mixingCoeffs[k] = priorAlpha + clusterWeight[k];
        }
    }


    private double[] calcExQlogMixing(double[] mixingCoeffs) {
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
    public double getZScoreEquivalent(double zscore) {
        return 0;
    }

    @Override
    public double[] getClusterWeights() {
        return mixingCoeffs;
    }

    @Override
    public List<RealVector> getClusterCenters() {
        return atomLoc;
    }

    @Override
    public List<RealMatrix> getClusterCovariances() {
        List<RealMatrix> covariances = new ArrayList<>(K);
        for (int k = 0; k < this.K; k++) {
            covariances.add(AlgebraUtils.invertMatrix(atomOmega.get(k).scalarMultiply(atomDOF[k])));
        }
        return covariances;
    }

    public double[] getPriorAdjustedWeights() {
        return clusterWeight;
    }

    @Override
    public double[] getClusterProbabilities(Datum d) {
        double[] probas = new double[K];
        double[] weights = getClusterWeights();
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
