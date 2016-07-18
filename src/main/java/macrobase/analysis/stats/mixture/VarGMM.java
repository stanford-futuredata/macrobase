package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.cluster.KMeans;
import macrobase.analysis.stats.distribution.MultivariateTDistribution;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A class that combines methods for all variational inference
 * # subclasses that use a type of Gaussian Mixture Model
 */
public class VarGMM {
    private static final Logger log = LoggerFactory.getLogger(VarGMM.class);

    public static final double ZERO_LOG_SCORE = -10000;
    protected NormalWishartClusters clusters;
    protected MixingComponents mixingComponents;
    protected List<MultivariateTDistribution> predictiveDistributions;
    protected final double progressCutoff;
    protected final int maxIterationsToConverge;
    protected MacroBaseConf conf;
    protected double trainTestSplit;
    protected final String initialClusterCentersFile;
    private int desiredMinibatchSize;
    private double delay;
    private double forgettingRate;

    protected int K;
    protected boolean isFinite;

    public enum AtomInitializationAlgorithm {
        GONZALEZ,
        KMEANS_RANDOM,
    }


    public VarGMM(MacroBaseConf conf) throws ConfigurationException {
        this.conf = conf;
        progressCutoff = conf.getDouble(MacroBaseConf.ITERATIVE_PROGRESS_CUTOFF_RATIO, MacroBaseDefaults.ITERATIVE_PROGRESS_CUTOFF_RATIO);
        maxIterationsToConverge = conf.getInt(MacroBaseConf.MAX_ITERATIONS_TO_CONVERGE, MacroBaseDefaults.MAX_ITERATIONS_TO_CONVERGE);
        trainTestSplit = conf.getDouble(MacroBaseConf.TRAIN_TEST_SPLIT, MacroBaseDefaults.TRAIN_TEST_SPLIT);
        log.debug("max iter = {}", maxIterationsToConverge);
        desiredMinibatchSize = conf.getInt(MacroBaseConf.SVI_MINIBATCH_SIZE, MacroBaseDefaults.SVI_MINIBATCH_SIZE);
        delay = conf.getDouble(MacroBaseConf.SVI_DELAY, MacroBaseDefaults.SVI_DELAY);
        forgettingRate = conf.getDouble(MacroBaseConf.SVI_FORGETTING_RATE, MacroBaseDefaults.SVI_FORGETTING_RATE);
        this.initialClusterCentersFile = conf.getString(MacroBaseConf.MIXTURE_CENTERS_FILE, null);

        switch (conf.getTransformType()) {
            case SVI_GMM:
                log.info("Will train with stochastic algorithm");
            case MEAN_FIELD_GMM:
                log.info("Using Finite mixture of Gaussians (Bayesian algorithm) transform.");
                K = conf.getInt(MacroBaseConf.NUM_MIXTURES, MacroBaseDefaults.NUM_MIXTURES);
                mixingComponents = new MultiComponents(0.1, K);
                isFinite = true;
                break;
            case SVI_DPGMM:
                log.info("Will train with stochastic algorithm");
            case MEAN_FIELD_DPGMM:
                log.info("Using Infinite mixture of Gaussians (DP Bayesian algorithm) transform.");
                K = conf.getInt(MacroBaseConf.DPM_TRUNCATING_PARAMETER, MacroBaseDefaults.DPM_TRUNCATING_PARAMETER);
                double concentrationParameter = conf.getDouble(MacroBaseConf.DPM_CONCENTRATION_PARAMETER, MacroBaseDefaults.DPM_CONCENTRATION_PARAMETER);
                mixingComponents = new DPComponents(concentrationParameter, K);
                isFinite = false;
                break;
            default:
                throw new RuntimeException("Unhandled transform class!" + conf.getTransformType());
        }
    }
    public void kmeansInitialize(List<Datum> trainData) {
        KMeans kMeans = new KMeans(K);
        kMeans.emIterate(trainData, conf.getRandom());
    }

    public void initialize(List<Datum> trainData) {
        clusters = new NormalWishartClusters(K, trainData.get(0).getMetrics().getDimension());
        if (isFinite) {
            clusters.initializeBaseForFinite(trainData);
            clusters.initializeAtomsForFinite(trainData, initialClusterCentersFile, conf.getRandom());
        } else {
            clusters.initializeBaseForDP(trainData);
            clusters.initializeAtomsForDP(trainData, initialClusterCentersFile, conf.getRandom());
        }
    }

    public void sviLoop(List<Datum> data) {
        double pace = Math.pow(delay, -forgettingRate);
        VariationalInference.loopStochVar(mixingComponents, clusters, data, desiredMinibatchSize, pace);
    }

    public void sviTrainToConvergeAndMonitor(List<Datum> trainData, List<Datum> monitorData) {

        double logLikelihood = -Double.MAX_VALUE;
        for (int iter = 1; ; iter++) {
            double pace = Math.pow(iter + delay, -forgettingRate);
            log.debug("pace = {}", pace);
            log.debug("centers = {}", clusters.getMAPLocations());
            log.debug("covariances = {}", clusters.getMAPCovariances());
            log.debug("weights = {}", mixingComponents.getNormalizedClusterProportions());

            VariationalInference.loopStochVar(mixingComponents, clusters, trainData, desiredMinibatchSize, pace);

            double oldLogLikelihood = logLikelihood;
            logLikelihood = this.calculateLogLikelihood(trainData, mixingComponents, clusters);
            double testLogLikelihood = this.calculateLogLikelihood(monitorData, mixingComponents, clusters);
            log.debug("test loglike = {}", testLogLikelihood);
            if (this.checkTermination(logLikelihood, oldLogLikelihood, iter)) {
                log.debug("centers = {}", clusters.getMAPLocations());
                log.debug("covariances = {}", clusters.getMAPCovariances());
                log.debug("weights = {}", mixingComponents.getNormalizedClusterProportions());
                return;
            }
            if (Math.abs((oldLogLikelihood - logLikelihood) / logLikelihood) < 1e-8) {
                log.debug("centers = {}", clusters.getMAPLocations());
                log.debug("covariances = {}", clusters.getMAPCovariances());
                log.debug("weights = {}", mixingComponents.getNormalizedClusterProportions());
                return;
            }
        }
    }

    public boolean checkTermination(double logLikelihood, double oldLogLikelihood, int iteration) {
        log.debug("average point log likelihood after iteration {} is {}", iteration, logLikelihood);

        if (iteration >= maxIterationsToConverge) {
            log.debug("Breaking because have already run {} iterations", iteration);
            return true;
        }

        double improvement = (logLikelihood - oldLogLikelihood) / (-logLikelihood);
        if (improvement >= 0 && improvement < progressCutoff) {
            log.debug("Breaking because improvement was {} percent", improvement * 100);
            return true;
        } else {
            log.debug("improvement is : {} percent", improvement * 100);
        }
        log.debug(".........................................");
        return false;
    }


    public double meanLogLike(List<Datum> data) {
        return calculateLogLikelihood(data, mixingComponents, clusters);
    }

    public List<RealMatrix> getClusterCovariances() {
        return clusters.getMAPCovariances();
    }

    public List<RealVector> getClusterCenters() {
        return clusters.getMAPLocations();
    }

    public double calculateLogLikelihood(List<Datum> data, MixingComponents mixingComonents, NormalWishartClusters clusters) {
        predictiveDistributions = clusters.constructPredictiveDistributions();
        double logLikelihood = 0;
        for (Datum d : data) {
            logLikelihood += score(d);
        }
        return logLikelihood / data.size();
    }

    /**
     * @param datum
     * @return log probability density of the given datum (or -10000 if probability density is 0)
     */
    public double score(Datum datum) {
        double density = 0;
        double[] cc =  mixingComponents.getNormalizedClusterProportions();
        for (int i = 0; i < predictiveDistributions.size(); i++) {
            density += cc[i] * predictiveDistributions.get(i).density(datum.getMetrics());
        }
        if (density == 0) {
            return this.ZERO_LOG_SCORE;
        }
        if (Double.isNaN(Math.log(density))) {
            log.debug("total density = {}", density);
            for (int i = 0; i < predictiveDistributions.size(); i++) {
                log.debug("cc = {}, density = {}", cc[i], predictiveDistributions.get(i).density(datum.getMetrics()));
            }
        }
        return Math.log(density);
    }

    /**
     * Calculates probabilities of a cluster belonging to each of the clusters.
     * Equals the weighted probabilities of data coming from each of the clusters.
     */
    public double[] getClusterProbabilities(Datum d) {
        double[] weights = mixingComponents.getNormalizedClusterProportions();
        double[] probas = new double[weights.length];

        double total = 0;
        for (int i = 0; i < weights.length; i++) {
            probas[i] = weights[i] * predictiveDistributions.get(i).density(d.getMetrics());
            total += probas[i];
        }
        for (int i = 0; i < weights.length; i++) {
            probas[i] /= total;
        }
        return probas;
    }

    public NormalWishartClusters getClusters() {
        return clusters;
    }

    public MixingComponents getMixingComponents() {
        return mixingComponents;
    }

}
