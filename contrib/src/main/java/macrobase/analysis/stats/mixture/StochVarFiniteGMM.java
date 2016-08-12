package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StochVarFiniteGMM extends FiniteGMM {
    private static final Logger log = LoggerFactory.getLogger(StochVarFiniteGMM.class);
    private final int desiredMinibatchSize;
    private final double delay;
    private final double forgettingRate;

    public StochVarFiniteGMM(MacroBaseConf conf) {
        super(conf);
        desiredMinibatchSize = conf.getInt(GMMConf.SVI_MINIBATCH_SIZE, GMMConf.SVI_MINIBATCH_SIZE_DEFAULT);
        delay = conf.getDouble(GMMConf.SVI_DELAY, GMMConf.SVI_DELAY_DEFAULT);
        forgettingRate = conf.getDouble(GMMConf.SVI_FORGETTING_RATE, GMMConf.SVI_FORGETTING_RATE_DEFAULT);
    }

    @Override
    public void trainTest(List<Datum> trainData, List<Datum> testData) {
        mixingComponents = new MultiComponents(0.1, K);
        clusters = new NormalWishartClusters(K, trainData.get(0).metrics().getDimension());
        clusters.initializeBaseForFinite(trainData);
        clusters.initializeAtomsForFinite(trainData, initialClusterCentersFile, conf.getRandom());

        VariationalInference.trainTestStochastic(this, trainData, testData, mixingComponents, clusters, desiredMinibatchSize, delay, forgettingRate);
    }
}
