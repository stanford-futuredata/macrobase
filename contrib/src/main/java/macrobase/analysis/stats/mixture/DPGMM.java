package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
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
        T = conf.getInt(GMMConf.DPM_TRUNCATING_PARAMETER, GMMConf.DPM_TRUNCATING_PARAMETER_DEFAULT);
        double concentrationParameter = conf.getDouble(GMMConf.DPM_CONCENTRATION_PARAMETER, GMMConf.DPM_CONCENTRATION_PARAMETER_DEFAULT);
        mixingComponents = new DPComponents(concentrationParameter, T);
    }

    public void trainTest(List<Datum> trainData, List<Datum> testData) {
        // 0. Initialize all approximating factors
        clusters = new NormalWishartClusters(T, trainData.get(0).metrics().getDimension());
        clusters.initializeBaseForDP(trainData);
        clusters.initializeAtomsForDP(trainData, initialClusterCentersFile, conf.getRandom());

        VariationalInference.trainTestMeanField(this, trainData, testData, mixingComponents, clusters);
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
