package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Mean-Field Variational Inference for Dirichlet Process Gaussian Mixture Model.
 */
public class DPGMM extends VarGMM {
    private static final Logger log = LoggerFactory.getLogger(DPGMM.class);
    protected DPComponents mixingComponents;

    protected final int T;

    public DPGMM(MacroBaseConf conf) {
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
        VariationalInference.trainMeanField(this, data, mixingComponents, clusters);
    }

    @Override
    public double[] getClusterProportions() {
        return mixingComponents.getNormalizedClusterProportions();
    }

    @Override
    protected double[] getNormClusterContrib() {
        return mixingComponents.getNormalizedClusterProportions();
    }
}
