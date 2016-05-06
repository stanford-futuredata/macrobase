package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Variational Dirichlet Process Mixture of Gaussians.
 */
public class VariationalDPGMM extends VarGMM {
    private static final Logger log = LoggerFactory.getLogger(VariationalDPGMM.class);
    private DPComponents mixingComponents;

    private final int T;

    public VariationalDPGMM(MacroBaseConf conf) {
        super(conf);
        T = conf.getInt(MacroBaseConf.DPM_TRUNCATING_PARAMETER, MacroBaseDefaults.DPM_TRUNCATING_PARAMETER);
        double concentrationParameter = conf.getDouble(MacroBaseConf.DPM_CONCENTRATION_PARAMETER, MacroBaseDefaults.DPM_CONCENTRATION_PARAMETER);
        mixingComponents = new DPComponents(concentrationParameter, T);
    }

    @Override
    public void train(List<Datum> data) {
        // 0. Initialize all approximating factors
        log.debug("training locally");
        clusters = new NormalWishartClusters(T, data.get(0).getMetrics().getDimension());
        clusters.initializeBaseForDP(data);
        clusters.initializeAtomsForDP(data, conf.getRandom());

        log.debug("actual training");
        MeanFieldVariationalInference.train(this, data, mixingComponents, clusters);
    }

    @Override
    public double score(Datum datum) {
        double density = 0;
        double[] stickLengths = mixingComponents.getClusterProportions();
        for (int i = 0; i < predictiveDistributions.size(); i++) {
            density += stickLengths[i] * predictiveDistributions.get(i).density(datum.getMetrics());
        }
        return Math.log(density);
    }

    @Override
    public double[] getClusterProportions() {
        return mixingComponents.getClusterProportions();
    }

    @Override
    public double[] getClusterProbabilities(Datum d) {
        double[] probas = new double[T];
        double[] weights = getClusterProportions();
        double normalizingConstant = 0;
        for (int i = 0; i < T; i++) {
            probas[i] = weights[i] * predictiveDistributions.get(i).density(d.getMetrics());
            normalizingConstant += probas[i];
        }
        for (int i = 0; i < T; i++) {
            probas[i] /= normalizingConstant;
        }
        return probas;
    }
}
