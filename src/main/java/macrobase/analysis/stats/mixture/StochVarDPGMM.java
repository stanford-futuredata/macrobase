package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.TrainTestSpliter;
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
        desiredMinibatchSize = conf.getInt(MacroBaseConf.SVI_MINIBATCH_SIZE, MacroBaseDefaults.SVI_MINIBATCH_SIZE);
        delay = conf.getDouble(MacroBaseConf.SVI_DELAY, MacroBaseDefaults.SVI_DELAY);
        forgettingRate = conf.getDouble(MacroBaseConf.SVI_FORGETTING_RATE, MacroBaseDefaults.SVI_FORGETTING_RATE);
    }

    @Override
    public void train(List<Datum> data) {
        // 0. Initialize all approximating factors
        log.debug("training locally");
        clusters = new NormalWishartClusters(T, data.get(0).getMetrics().getDimension());
        clusters.initializeBaseForDP(data);
        clusters.initializeAtomsForDP(data, conf.getRandom());

        log.debug("actual training");
        if ( trainTestSplit > 0 && trainTestSplit < 1) {
            TrainTestSpliter splitter = new TrainTestSpliter(data, trainTestSplit, conf.getRandom());
            VariationalInference.trainTestStochastic(this, splitter.getTrainData(), splitter.getTestData(), mixingComponents, clusters, desiredMinibatchSize, delay, forgettingRate);
        } else {
            VariationalInference.trainStochastic(this, data, mixingComponents, clusters, desiredMinibatchSize, delay, forgettingRate);
        }
    }
}
