package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.MultivariateNormal;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.util.TrainTestSpliter;
import org.apache.commons.math3.linear.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ExpectMaxGMM extends BatchMixtureModel {
    private static final Logger log = LoggerFactory.getLogger(ExpectMaxGMM.class);

    private int K;  // Number of mixture components
    private double[] phi;  // Mixing coefficients, K vector
    private List<RealVector> mu;  // Means of Gaussians
    private List<RealMatrix> sigma;  // Covariances of Gaussians
    private List<MultivariateNormal> mixtureDistributions;
    private double EMCutoffProgress;

    public ExpectMaxGMM(MacroBaseConf conf) {
        super(conf);
        this.K = conf.getInt(GMMConf.NUM_MIXTURES, GMMConf.NUM_MIXTURES_DEFAULT);
        this.EMCutoffProgress = conf.getDouble(GMMConf.ITERATIVE_PROGRESS_CUTOFF_RATIO, GMMConf.ITERATIVE_PROGRESS_CUTOFF_RATIO_DEFAULT);
        log.debug("created Gaussian MM with {} mixtures", this.K);
    }

    @Override
    public void train(List<Datum> data) {
        if ( trainTestSplit > 0 && trainTestSplit < 1) {
            TrainTestSpliter splitter = new TrainTestSpliter(data, trainTestSplit, conf.getRandom());
            trainTestEM(splitter.getTrainData(), splitter.getTestData());
        } else {
            trainTestEM(data, data);
        }
    }

    private void trainTestEM(List<Datum> trainData, List<Datum> testData) {
        int N = trainData.size();
        log.debug("N = {}", N);
        int dimensions = trainData.get(0).metrics().getDimension();
        // 1. Initialize the means and covariances and mixing coefficients,
        //    and evaluate the initial value of the log likelihood.
        mu = new ArrayList<>(this.K);
        phi = new double[K];
        mixtureDistributions = new ArrayList<>(K);
        sigma = new ArrayList<>(K);
        // Initialize cluster means using Gonzalez algorithm (takes O(KN) time).
        // ..Almost the same as one iteration of EM
        // Picks a random point, than each next point is the one
        // that's the furthest away from the chosen points so far.
        // Picking points uniformly does not work, because it sometimes leads
        // to a local maximum in EM optimization where two cluster are replaces with
        // twice the cluster that represents both.
        mu = this.gonzalezInitializeMixtureCenters(trainData, this.K, conf.getRandom());
        for (int k = 0; k < K; k++) {
            sigma.add(MatrixUtils.createRealIdentityMatrix(dimensions));
            mixtureDistributions.add(new MultivariateNormal(mu.get(k), sigma.get(k)));
            phi[k] = 1. / K;
        }

        // EM algorithm;
        double logLikelihood = -Double.MAX_VALUE;
        for (int iteration = 0; iteration < maxIterationsToConverge; iteration++) {
            // 2. E step. Evaluate the responsibilities using the current parameter values.
            double[][] gamma = new double[N][K];
            double[] clusterWeight = new double[N];  // N_k (Bishop)
            for (int n = 0; n < N; n++) {
                double normalizingConstant = 0;
                for (int k = 0; k < K; k++) {
                    gamma[n][k] = phi[k] * mixtureDistributions.get(k).density(trainData.get(n).metrics());
                    normalizingConstant += gamma[n][k];
                }
                for (int k = 0; k < K; k++) {
                    gamma[n][k] /= normalizingConstant;
                    clusterWeight[k] += gamma[n][k];
                }
            }

            // 3. M step. Re-estimate the parameters using the current responsibilities.
            for (int k = 0; k < K; k++) {
                RealVector newMu = new ArrayRealVector(dimensions);
                for (int n = 0; n < N; n++) {
                    newMu = newMu.add(trainData.get(n).metrics().mapMultiply(gamma[n][k]));
                }
                newMu = newMu.mapDivide(clusterWeight[k]);
                RealMatrix newSigma = new BlockRealMatrix(dimensions, dimensions);
                mu.set(k, newMu);
                for (int n = 0; n < N; n++) {
                    RealVector _diff = trainData.get(n).metrics().subtract(newMu);
                    newSigma = newSigma.add(_diff.outerProduct(_diff).scalarMultiply(gamma[n][k]));
                }
                newSigma = newSigma.scalarMultiply(1. / clusterWeight[k]);
                sigma.set(k, newSigma);
                phi[k] = clusterWeight[k] / N;
            }

            // 4. Evaluate the log likelihood
            for (int k = 0; k < this.K; k++) {
                mixtureDistributions.set(k, new MultivariateNormal(mu.get(k), sigma.get(k)));
            }

            double oldLogLikelihood = logLikelihood;
            logLikelihood = 0;
            for (int n = 0; n < testData.size(); n++) {
                logLikelihood += score(testData.get(n));
            }
            logLikelihood /= testData.size();

            log.debug("per point log likelihood after iteration {} is {}", iteration, logLikelihood);

            log.debug("cluster likelihoods are: {}", phi);
            log.debug("cluster centers are at {}", mu);
            log.debug("cluster covariances are at {}", sigma);

            double improvement = (logLikelihood - oldLogLikelihood) / (-logLikelihood);
            if (improvement >= 0 && improvement < this.EMCutoffProgress) {
                log.debug("Breaking because improvement was {} percent", improvement * 100);
                break;
            } else {
                log.debug("improvement is : {}%", improvement * 100);
            }
        }
    }

    /**
     * @param datum
     * @return log probability density of the given datum
     */
    @Override
    public double score(Datum datum) {
        double probability = 0;
        for (int k = 0; k < K; k++) {
            probability += phi[k] * mixtureDistributions.get(k).density(datum.metrics());
        }
        return Math.log(probability);
    }

    @Override
    public List<RealVector> getClusterCenters() {
        return mu;
    }

    @Override
    public double[] getClusterProportions() {
        return phi;
    }

    @Override
    public List<RealMatrix> getClusterCovariances() {
        return sigma;
    }

    @Override
    public double[] getClusterProbabilities(Datum d) {
        double[] probas = new double[K];
        double normalizingConstant = 0;
        for (int k = 0; k < K; k++) {
            probas[k] = phi[k] * mixtureDistributions.get(k).density(d.metrics());
            normalizingConstant += probas[k];
        }
        for (int k = 0; k < K; k++) {
            probas[k] /= normalizingConstant;
        }
        return probas;
    }

}

