package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
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
        desiredMinibatchSize = conf.getInt(MacroBaseConf.SVI_MINIBATCH_SIZE, MacroBaseDefaults.SVI_MINIBATCH_SIZE);
        delay = conf.getDouble(MacroBaseConf.SVI_DELAY, MacroBaseDefaults.SVI_DELAY);
        forgettingRate = conf.getDouble(MacroBaseConf.SVI_FORGETTING_RATE, MacroBaseDefaults.SVI_FORGETTING_RATE);
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
        VariationalInference.trainStochastic(this, data, mixingComponents, clusters, desiredMinibatchSize, delay, forgettingRate);
    }
}
