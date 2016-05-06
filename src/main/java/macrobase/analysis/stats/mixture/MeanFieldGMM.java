package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.Wishart;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.AlgebraUtils;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;

public abstract class MeanFieldGMM extends BatchMixtureModel {
    private static final Logger log = LoggerFactory.getLogger(MeanFieldGMM.class);
    protected double progressCutoff;

    // Parameters for Base Distribution, which is Wishart-Gaussian
    protected double baseNu;
    protected RealMatrix baseOmega;
    protected RealMatrix baseOmegaInverse;  // Use inverse of baseOmega, since it is used in the update equations.
    protected double baseBeta;
    protected RealVector baseLoc;

    // Variables governing atoms (components).
    // Omega and atomDOF for Wishart distribution for the precision matrix of the clusters.
    protected double atomDOF[];
    protected List<RealMatrix> atomOmega;
    // Parameters for Normal distribution for atom position, N(atomLocation, (atomBeta * Lambda))
    // where Lambda is Wishart distributed given parameters above.
    protected double atomBeta[];
    protected List<RealVector> atomLoc;

    // Useful constants
    protected int D;
    protected double halfDimensionLn2Pi;
    protected double dimensionLn2;

    // Mean-Field iterative algorithm maximum iterations.
    protected final int maxIterationsToConverge;

    // TODO: right now FiniteGMM needs this
    protected double[] clusterWeight;

    public MeanFieldGMM(MacroBaseConf conf) {
        super(conf);
        this.progressCutoff = conf.getDouble(MacroBaseConf.EM_CUTOFF_PROGRESS, MacroBaseDefaults.EM_CUTOFF_PROGRESS);
        maxIterationsToConverge = conf.getInt(MacroBaseConf.MIXTURE_MAX_ITERATIONS_TO_CONVERGE, MacroBaseDefaults.MIXTURE_MAX_ITERATIONS_TO_CONVERGE);
    }

    protected void initConstants(List<Datum> data) {
        D = data.get(0).getMetrics().getDimension();
        halfDimensionLn2Pi = 0.5 * D * Math.log(2 * Math.PI);
        dimensionLn2 = D * Math.log(2);
    }

    // TODO: figure out a combine initalization strategy that works both for finite and DP GMMs.
    protected abstract void initializeBaseNormalWishart(List<Datum> data);

    protected abstract void initializeBaseMixing();

    protected abstract void initializeSticks();

    protected abstract void initializeAtoms(List<Datum> data);

    protected abstract void updateSticks(double[][] r);

    protected abstract double[] calcExQlogMixing();

    protected abstract void updatePredictiveDistributions();

    //public void train(List<Datum> data, MixingComponents mixingComonents, NormalWishartClusters clusters, int maxIter, double improvementCutoff) {
    //    double[] exLnMixingContribution;
    //    double[] lnPrecision;
    //    double[][] dataLogLike;
    //    double[][] r;

    //    int N = data.size();
    //    double logLikelihood = -Double.MAX_VALUE;
    //    for (int iter = 1; ; iter++) {
    //        // Step 1. update local variables
    //        exLnMixingContribution = mixingComonents.calcExpectationLog();
    //        lnPrecision = clusters.calculateExLogPrecision();
    //        dataLogLike = clusters.calcLogLikelyFixedPrec(data);
    //        r = normalizeLogProbas(exLnMixingContribution, lnPrecision, dataLogLike);

    //        // Step 2. update global variables
    //        mixingComonents.update(r);
    //        clusters.update(data, r);

    //        // TODO:!!!
    //        updatePredictiveDistributions();

    //        if (iter>= maxIterationsToConverge) {
    //            log.debug("Breaking because have already run {} iterations", iter);
    //            break;
    //        }

    //        double oldLogLikelihood = logLikelihood;
    //        logLikelihood = 0;
    //        for (int n = 0; n < N; n++) {
    //            logLikelihood += score(data.get(n));
    //        }

    //        double improvement = (logLikelihood - oldLogLikelihood) / (-logLikelihood);
    //        if (improvement >= 0 && improvement < this.progressCutoff) {
    //            log.debug("Breaking because improvement was {} percent", improvement * 100);
    //            break;
    //        } else {
    //            log.debug("improvement is : {}%", improvement * 100);
    //        }
    //        log.debug(".........................................");

    //    }
    //}

        /**
         * @param data - data to train on
         * @param K    - run inference with K clusters
         */

    public void train(List<Datum> data, int K) {
        int N = data.size();
        // 0. Initialize all approximating factors
        initConstants(data);
        initializeBaseNormalWishart(data);
        initializeBaseMixing();
        initializeSticks();
        initializeAtoms(data);

        List<Wishart> wisharts;
        // density of each point with respect to each mixture component.
        double[][] r = new double[N][K];

        double logLikelihood = -Double.MAX_VALUE;
        for (int iteration = 1; ; iteration++) {

            // Useful to keep everything tidy.
            wisharts = constructWisharts(atomOmega, atomDOF);

            // 1. calculate expectation of densities of each point coming from individual clusters - r[n][k]
            // 1. Reevaluate r[][]

            // Calculate mixing coefficient log expectation
            double[] exLnMixingContribution = calcExQlogMixing();

            double[][] dataLogLike = calcLogLikelihoodFixedAtoms(data, atomLoc, atomBeta, atomOmega, atomDOF);

            for (int n = 0; n < N; n++) {
                double normalizingConstant = 0;
                for (int k = 0; k < K; k++) {
                    r[n][k] = Math.exp(exLnMixingContribution[k] + 0.5 * wisharts.get(k).getExpectationLogDeterminantLambda() + dataLogLike[n][k]);
                    normalizingConstant += r[n][k];
                }
                for (int k = 0; k < atomLoc.size(); k++) {
                    if (normalizingConstant > 0) {
                        r[n][k] /= normalizingConstant;
                    }
                }
            }

            // 2. Reevaluate clusters based on densities that we have for each point.
            // 2. Reevaluate atoms and stick lengths.

            updateSticks(r);
            updateAtoms(r, data);

            // TODO: this should be somehow moved to FiniteGMM, or testing for this should be removed.
            clusterWeight = calculateClusterWeights(r);

            updatePredictiveDistributions();

            double oldLogLikelihood = logLikelihood;
            logLikelihood = 0;
            for (int n = 0; n < N; n++) {
                logLikelihood += score(data.get(n));
            }

            log.debug("log likelihood after iteration {} is {}", iteration, logLikelihood);

            if (iteration >= maxIterationsToConverge) {
                log.debug("Breaking because have already run {} iterations", iteration);
                break;
            }

            double improvement = (logLikelihood - oldLogLikelihood) / (-logLikelihood);
            if (improvement >= 0 && improvement < this.progressCutoff) {
                log.debug("Breaking because improvement was {} percent", improvement * 100);
                break;
            } else {
                log.debug("improvement is : {}%", improvement * 100);
            }
            log.debug(".........................................");
        }
    }

    protected double[][] normalizeLogProbas(double[] lnMixing, double[] lnPrecision, double[][] dataLogLike) {
        double[][] r = new double[dataLogLike.length][lnMixing.length];
        for (int n = 0; n < dataLogLike.length; n++) {
            double normalizingConstant = 0;
            for (int k = 0; k < lnMixing.length; k++) {
                r[n][k] = Math.exp(lnMixing[k] + lnPrecision[k] + dataLogLike[n][k]);
                normalizingConstant += r[n][k];
            }
            for (int k = 0; k < atomLoc.size(); k++) {
                if (normalizingConstant > 0) {
                    r[n][k] /= normalizingConstant;
                }
            }
        }
        return r;
    }

    protected static double[] calculateExLogPrecision(List<RealMatrix> omega, double[] dof) {
        int K = omega.size();
        double[] lnPrecision = new double[K];
        for (int i = 0; i < K; i++) {
            lnPrecision[i] = 0.5 * (new Wishart(omega.get(i), dof[i])).getExpectationLogDeterminantLambda();
        }
        return lnPrecision;
    }

    protected static List<RealMatrix> calculateQuadraticForms(List<Datum> data, List<RealVector> clusterMean, double[][] r) {
        int D = data.get(0).getMetrics().getDimension();
        int K = clusterMean.size();
        int N = data.size();
        List<RealMatrix> quadForm = new ArrayList<>(K);
        for (int k = 0; k < K; k++) {
            RealMatrix form = new BlockRealMatrix(D, D);
            for (int n = 0; n < N; n++) {
                RealVector _diff = data.get(n).getMetrics().subtract(clusterMean.get(k));
                form = form.add(_diff.outerProduct(_diff).scalarMultiply(r[n][k]));
            }
            quadForm.add(form);
        }
        return quadForm;
    }

    protected static List<Wishart> constructWisharts(List<RealMatrix> omega, double[] dof) {
        int num = omega.size();
        List<Wishart> wisharts = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            wisharts.add(new Wishart(omega.get(i), dof[i]));
        }
        return wisharts;
    }

    protected static List<RealVector> calculateWeightedSums(List<Datum> data, double[][] r) {
        int N = data.size();
        int K = r[0].length;
        int D = data.get(0).getMetrics().getDimension();
        List<RealVector> sums = new ArrayList<>(K);
        for (int k = 0; k < K; k++) {
            RealVector sum = new ArrayRealVector(D);
            for (int n = 0; n < N; n++) {
                sum = sum.add(data.get(n).getMetrics().mapMultiply(r[n][k]));
            }
            sums.add(sum);
        }
        return sums;
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

    protected double[][] calcLogLikelihoodFixedAtoms(List<Datum> data, List<RealVector> atomLoc, double[] atomBeta, List<RealMatrix> atomOmega, double[] atomDOF) {
        int N = data.size();
        int K = atomLoc.size();
        double[][] loglike = new double[N][K];
        for (int k = 0; k < K; k++) {
            for (int n = 0; n < N; n++) {
                RealVector _diff = data.get(n).getMetrics().subtract(atomLoc.get(k));
                loglike[n][k] = -halfDimensionLn2Pi - 0.5 * (
                        D / atomBeta[k] + atomDOF[k] * _diff.dotProduct(atomOmega.get(k).operate(_diff)));
            }
        }
        return loglike;
    }

    protected void updateAtoms(double[][] r, List<Datum> data) {
        double[] clusterWeight = calculateClusterWeights(r);
        int K = atomLoc.size();
        List<RealVector> weightedSum = calculateWeightedSums(data, r);
        List<RealVector> clusterMean = new ArrayList<>(K);
        for (int k = 0; k < K; k++) {
            if (clusterWeight[k] > 0) {
                clusterMean.add(weightedSum.get(k).mapDivide(clusterWeight[k]));
            } else {
                clusterMean.add(weightedSum.get(k));
                log.debug("weighted sum = {} (should be 0)", weightedSum.get(k));
            }
        }
        List<RealMatrix> quadForm = calculateQuadraticForms(data, clusterMean, r);
        log.debug("clusterWeights: {}", clusterWeight);

        for (int k = 0; k < K; k++) {
            atomBeta[k] = baseBeta + clusterWeight[k];
            atomLoc.set(k, baseLoc.mapMultiply(baseBeta).add(weightedSum.get(k)).mapDivide(atomBeta[k]));
            atomDOF[k] = baseNu + 1 + clusterWeight[k];
            RealVector adjustedMean = clusterMean.get(k).subtract(baseLoc);
            //log.debug("adjustedMean: {}", adjustedMean);
            RealMatrix wInverse = baseOmegaInverse
                    .add(quadForm.get(k))
                    .add(adjustedMean.outerProduct(adjustedMean).scalarMultiply(baseBeta * clusterWeight[k] / (baseBeta + clusterWeight[k])));
            //log.debug("wInverse: {}", wInverse);
            atomOmega.set(k, AlgebraUtils.invertMatrix(wInverse));
        }
    }

    // Mixture Model Methods
    @Override
    public List<RealVector> getClusterCenters() {
        return atomLoc;
    }

    @Override
    public List<RealMatrix> getClusterCovariances() {
        List<RealMatrix> covariances = new ArrayList<>(atomOmega.size());
        for (int i = 0; i < atomOmega.size(); i++) {
            covariances.add(AlgebraUtils.invertMatrix(atomOmega.get(i).scalarMultiply(atomDOF[i])));
        }
        return covariances;
    }

    @Override
    public double getZScoreEquivalent(double zscore) {
        throw new NotImplementedException();
    }

}

