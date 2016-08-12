package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StochVarDPGMM extends DPGMM {
    private static final Logger log = LoggerFactory.getLogger(StochVarDPGMM.class);
    private final int desiredMinibatchSize;
    private final double delay;
    private final double forgettingRate;

    public StochVarDPGMM(MacroBaseConf conf) {
        super(conf);
        desiredMinibatchSize = conf.getInt(GMMConf.SVI_MINIBATCH_SIZE, GMMConf.SVI_MINIBATCH_SIZE_DEFAULT);
        delay = conf.getDouble(GMMConf.SVI_DELAY, GMMConf.SVI_DELAY_DEFAULT);
        forgettingRate = conf.getDouble(GMMConf.SVI_FORGETTING_RATE, GMMConf.SVI_FORGETTING_RATE_DEFAULT);
    }

    @Override
    public void trainTest(List<Datum> trainData, List<Datum> testData) {
        // 0. Initialize all approximating factors
        clusters = new NormalWishartClusters(T, trainData.get(0).metrics().getDimension());
        clusters.initializeBaseForDP(trainData);
        clusters.initializeAtomsForDP(trainData, initialClusterCentersFile, conf.getRandom());
        VariationalInference.trainTestStochastic(this, trainData, testData, mixingComponents, clusters, desiredMinibatchSize, delay, forgettingRate);
    }
}
