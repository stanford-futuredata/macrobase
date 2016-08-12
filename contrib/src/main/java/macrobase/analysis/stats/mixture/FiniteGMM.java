package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Fit Gaussian Mixture models using Variational Bayes
 */
public class FiniteGMM extends VarGMM {
    private static final Logger log = LoggerFactory.getLogger(FiniteGMM.class);

    protected int K;  // Number of mixture components

    // Components.
    protected MultiComponents mixingComponents;

    public FiniteGMM(MacroBaseConf conf) {
        super(conf);
        this.K = conf.getInt(GMMConf.NUM_MIXTURES, GMMConf.NUM_MIXTURES_DEFAULT);
        log.debug("created Gaussian MM with {} mixtures", this.K);
    }

    @Override
    public void trainTest(List<Datum> trainData, List<Datum> testData) {
        // 0. Initialize all approximating factors
        mixingComponents = new MultiComponents(0.1, K);
        clusters = new NormalWishartClusters(K, trainData.get(0).metrics().getDimension());
        clusters.initializeBaseForFinite(trainData);
        clusters.initializeAtomsForFinite(trainData, initialClusterCentersFile, conf.getRandom());

        VariationalInference.trainTestMeanField(this, trainData, testData, mixingComponents, clusters);
    }

    @Override
    public double[] getClusterProportions() {
        return mixingComponents.getCoeffs();
    }

    @Override
    protected double[] getNormClusterContrib() {
        return mixingComponents.getNormalizedClusterProportions();
    }

    public double[] getPriorAdjustedClusterProportions() {
        double[] mixingCoeffs = mixingComponents.getCoeffs();
        double sum = - mixingComponents.getPrior(); // to adjust for prior.
        for (double coeff : mixingCoeffs) {
            sum += coeff;
        }
        double[] proportions = new double[K];
        for (int i=0; i<K; i++) {
            proportions[i] = (mixingCoeffs[i] - mixingComponents.getPrior()) / sum;
        }
        return proportions;
    }

    @Override
    public double[] getClusterProbabilities(Datum d) {
        double[] probas = new double[K];
        double[] weights = getNormClusterContrib();
        double normalizingConstant = 0;
        for (int i = 0; i < K; i++) {
            probas[i] = weights[i] * predictiveDistributions.get(i).density(d.metrics());
            normalizingConstant += probas[i];
        }
        for (int i = 0; i < K; i++) {
            probas[i] /= normalizingConstant;
        }
        return probas;
    }
}
