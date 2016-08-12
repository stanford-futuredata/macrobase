package macrobase.analysis.stats.mixture;

import macrobase.analysis.stats.distribution.MultivariateTDistribution;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.util.TrainTestSpliter;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A class that combines methods for all variational inference
 # subclasses that use a type of Gaussian Mixture Model
 */
public abstract class VarGMM extends BatchMixtureModel {
    private static final Logger log = LoggerFactory.getLogger(VarGMM.class);

    public static final double ZERO_LOG_SCORE = -10000;
    protected NormalWishartClusters clusters;
    protected List<MultivariateTDistribution> predictiveDistributions;

    protected abstract double[] getNormClusterContrib();

    public VarGMM(MacroBaseConf conf) {
        super(conf);
    }

    @Override
    public void train(List<Datum> data) {
        if ( trainTestSplit > 0 && trainTestSplit < 1) {
            TrainTestSpliter splitter = new TrainTestSpliter(data, trainTestSplit, conf.getRandom());
            trainTest(splitter.getTrainData(), splitter.getTestData());
        } else {
            trainTest(data, data);
        }
    }

    public abstract void trainTest(List<Datum> trainData, List<Datum> testData);


    @Override
    public List<RealMatrix> getClusterCovariances() {
        return clusters.getMAPCovariances();
    }

    @Override
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
    @Override
    public double score(Datum datum) {
        double density = 0;
        double[] cc = getNormClusterContrib();
        for (int i = 0; i < predictiveDistributions.size(); i++) {
            density += cc[i] * predictiveDistributions.get(i).density(datum.metrics());
        }
        if (density == 0) {
            return this.ZERO_LOG_SCORE;
        }
        return Math.log(density);
    }

    @Override
    /**
     * Calculates probabilities of a cluster belonging to each of the clusters.
     * Equals the weighted probabilities of data coming from each of the clusters.
     */
    public double[] getClusterProbabilities(Datum d) {
        double[] weights = getNormClusterContrib();
        double[] probas = new double[weights.length];

        double total = 0;
        for (int i = 0; i < weights.length; i++) {
            probas[i] = weights[i] * predictiveDistributions.get(i).density(d.metrics());
            total += probas[i];
        }
        for (int i = 0; i < weights.length; i++) {
            probas[i] /= total;
        }
        return probas;
    }

}
