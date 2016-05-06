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
public class VariationalGMM extends BatchMixtureModel {
    private static final Logger log = LoggerFactory.getLogger(VariationalGMM.class);
    private final String initialClusterCentersFile;

    private int K;  // Number of mixture components
    private double progressCutoff;

    private double priorAlpha;
    private double priorBeta;
    private RealVector priorM;
    private double priorNu;
    private RealMatrix priorW;
    private double[] alpha;
    private List<MultivariateTDistribution> predictiveDistributions;

    private double[] clusterWeight;  // N_k (Bishop)

    // Gaussian-Wishart distribution coefficients
    // Coefficients for K Gaussian distributions
    private List<RealVector> atomLoc;
    private double[] atomBeta;
    // Wishart distribution coefficients for Precision matrix (lambda)
    private List<RealMatrix> atomOmega;
    private double[] atomDOF;

    // Useful constants for each dataset.
    protected int D;
    protected double halfDimensionLn2Pi;


    public VariationalGMM(MacroBaseConf conf) {
        super(conf);
        this.K = conf.getInt(MacroBaseConf.NUM_MIXTURES, MacroBaseDefaults.NUM_MIXTURES);
        this.progressCutoff = conf.getDouble(MacroBaseConf.EM_CUTOFF_PROGRESS, MacroBaseDefaults.EM_CUTOFF_PROGRESS);
        log.debug("created Gaussian MM with {} mixtures", this.K);
        this.initialClusterCentersFile = conf.getString(MacroBaseConf.MIXTURE_CENTERS_FILE, null);
    }

    @Override
    public void train(List<Datum> data) {
        int N = data.size();
        D = data.get(0).getMetrics().getDimension();
        halfDimensionLn2Pi = 0.5 * D * Math.log(2 * Math.PI);
        List<Wishart> wisharts;

        //priorAlpha = 1. / dimensions;
        priorAlpha = 0.1;
        //priorBeta = 1. / dimensions;
        priorBeta = 0.1;
        priorM = new ArrayRealVector(D);
        // priorNu = 1. / dimensions;
        priorNu = 0.1;
        priorW = MatrixUtils.createRealIdentityMatrix(D);
        //priorW.setEntry(1, 1, 3);
        RealMatrix priorWInverse = AlgebraUtils.invertMatrix(priorW);

        alpha = new double[K];
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
            alpha[k] = 1. / K;
            atomBeta[k] = priorBeta;
            atomDOF[k] = priorNu;
            atomOmega.add(priorW);
        }

        List<RealVector> clusterMean;
        List<RealMatrix> S;
        // density of each point with respect to each mixture component.
        double[][] r = new double[N][K];

        double logLikelihood = -Double.MAX_VALUE;
        for (int iteration = 0; ; iteration++) {
            double[] ex_ln_phi = new double[K];
            double sum_alpha = 0;
            clusterMean = new ArrayList<>(K);
            S = new ArrayList<>(K);

            // Useful to keep everything tidy.
            wisharts = constructWisharts(atomOmega, atomDOF);

            // 1. calculate expectation of densities of each point coming from individual clusters - r[n][k]
            for (int k = 0; k < this.K; k++) {
                sum_alpha += alpha[k];
                clusterMean.add(new ArrayRealVector(D));
                S.add(new BlockRealMatrix(D, D));
            }

            for (int k = 0; k < this.K; k++) {
                ex_ln_phi[k] = Gamma.digamma(alpha[k] - Gamma.digamma(sum_alpha));
            }

            log.debug("clusterWeights: {}", clusterWeight);
            double _const = 0.5 * D * Math.log(2 * Math.PI);
            double ex_ln_xmu;
            for (int n = 0; n < N; n++) {
                double normalizingConstant = 0;
                for (int k = 0; k < this.K; k++) {
                    RealVector _diff = data.get(n).getMetrics().subtract(atomLoc.get(k));
                    ex_ln_xmu = D / atomBeta[k] + atomDOF[k] * _diff.dotProduct(atomOmega.get(k).operate(_diff));
                    r[n][k] = Math.exp(ex_ln_phi[k] + 0.5 * wisharts.get(k).getExpectationLogDeterminantLambda() - _const - 0.5 * ex_ln_xmu);
                    normalizingConstant += r[n][k];
                }
                for (int k = 0; k < this.K; k++) {
                    if (normalizingConstant == 0) {
                        continue;
                    }
                    r[n][k] /= normalizingConstant;
                }
            }

            clusterWeight = calculateClusterWeights(r);
            List<RealVector> weightedSum = calculateWeightedSums(data, r);

            // 2. Reevaluate clusters based on densities that we have for each point.
            log.debug("clusterWeights: {}", clusterWeight);
            for (int k = 0; k < this.K; k++) {
                clusterMean.set(k, weightedSum.get(k).mapDivide(clusterWeight[k]));
                for (int n = 0; n < N; n++) {
                    RealVector _diff = data.get(n).getMetrics().subtract(clusterMean.get(k));
                    S.set(k, S.get(k).add(_diff.outerProduct(_diff).scalarMultiply(r[n][k])));
                }
                S.set(k, S.get(k).scalarMultiply(1 / clusterWeight[k]));
            }

            for (int k = 0; k < this.K; k++) {
                alpha[k] = priorAlpha + clusterWeight[k];
                atomBeta[k] = priorBeta + clusterWeight[k];
                atomLoc.set(k, priorM.mapMultiply(priorBeta).add(clusterMean.get(k).mapMultiply(clusterWeight[k])).mapDivide(atomBeta[k]));
                atomDOF[k] = priorNu + 1 + clusterWeight[k];
                RealVector adjustedMean = clusterMean.get(k).subtract(priorM);
                log.debug("adjustedMean: {}", adjustedMean);
                log.debug("S: {}", S.get(k));
                RealMatrix wInverse = priorWInverse.add(
                        S.get(k).scalarMultiply(clusterWeight[k])).add(
                        adjustedMean.outerProduct(adjustedMean).scalarMultiply(priorBeta * clusterWeight[k] / (priorBeta + clusterWeight[k])));
                log.debug("wInverse: {}", wInverse);
                atomOmega.set(k, AlgebraUtils.invertMatrix(wInverse));
            }

            predictiveDistributions = new ArrayList<>(K);
            for (int k = 0; k < this.K; k++) {
                double scale = (atomDOF[k] + 1 - D) * atomBeta[k] / (1 + atomBeta[k]);
                RealMatrix ll = AlgebraUtils.invertMatrix(atomOmega.get(k).scalarMultiply(scale));
                // TODO: MultivariateTDistribution should support real values for 3rd parameters
                predictiveDistributions.add(new MultivariateTDistribution(atomLoc.get(k), ll, (int) (atomDOF[k] - 1 - D)));
            }

            log.debug("cluster means are at {}", clusterMean);
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

    protected static List<Wishart> constructWisharts(List<RealMatrix> omega, double[] dof) {
        int num = omega.size();
        List<Wishart> wisharts = new ArrayList<>(num);
        for (int i=0; i<num; i++) {
            wisharts.add(new Wishart(omega.get(i), dof[i]));
        }
        return wisharts;
    }

    private List<RealVector> calculateWeightedSums(List<Datum> data, double[][] r) {
        int N = data.size();
        List<RealVector> sums = new ArrayList<>(K);
        for (int k=0; k<K; k++) {
            RealVector sum = new ArrayRealVector(D);
            for (int n=0; n<N; n++) {
                sum = sum.add(data.get(n).getMetrics().mapMultiply(r[n][k]));
            }
            sums.add(sum);
        }
        return sums;
    }

    private double[] calculateClusterWeights(double[][] r) {
        int N = r.length;
        double[] clusterWeight = new double[K];
        for (int k=0; k<K; k++) {
            for (int n=0; n < N; n++) {
                clusterWeight[k] += r[n][k];
            }
        }
        return clusterWeight;
    }

    @Override
    public double score(Datum datum) {
        double density = 0;
        double sum_alpha = 0;
        for (int k = 0; k < this.K; k++) {
            // If the mixture is very improbable, skip.
            if (Math.abs(alpha[k] - priorAlpha) < 1e-4) {
                continue;
            }
            sum_alpha += alpha[k];
            density += alpha[k] * this.predictiveDistributions.get(k).density(datum.getMetrics());
        }
        return Math.log(density / sum_alpha);
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        return 0;
    }

    @Override
    public double[] getClusterWeights() {
        return alpha;
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
