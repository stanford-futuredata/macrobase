package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.TrainTestSpliter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Fit Gaussian Mixture models using Variational Bayes
 */
public class FiniteGMM extends VarGMM {
    private static final Logger log = LoggerFactory.getLogger(FiniteGMM.class);
    protected final String initialClusterCentersFile;

    protected int K;  // Number of mixture components

    // Components.
    protected MultiComponents mixingComponents;

    public FiniteGMM(MacroBaseConf conf) {
        super(conf);
        this.K = conf.getInt(MacroBaseConf.NUM_MIXTURES, MacroBaseDefaults.NUM_MIXTURES);
        log.debug("created Gaussian MM with {} mixtures", this.K);
        this.initialClusterCentersFile = conf.getString(MacroBaseConf.MIXTURE_CENTERS_FILE, null);
    }

    @Override
    public void train(List<Datum> data) {
        // 0. Initialize all approximating factors
        log.debug("training locally");
        mixingComponents = new MultiComponents(0.1, K);
        clusters = new NormalWishartClusters(K, data.get(0).getMetrics().getDimension());
        clusters.initializeBaseForFinite(data);
        clusters.initializeAtomsForFinite(data, initialClusterCentersFile, conf.getRandom());
        //clusters.initializeBase(baseLoc, baseBeta, baseOmega, baseNu);

        log.debug("actual training");
        if ( trainTestSplit > 0 && trainTestSplit < 1) {
            TrainTestSpliter splitter = new TrainTestSpliter(data, trainTestSplit, conf.getRandom());
            VariationalInference.trainTestMeanField(this, splitter.getTrainData(), splitter.getTestData(), mixingComponents, clusters);
        } else {
            VariationalInference.trainMeanField(this, data, mixingComponents, clusters);
        }
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
