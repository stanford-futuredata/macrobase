package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StochVarDPGMM extends DPGMM {
    private static final Logger log = LoggerFactory.getLogger(StochVarDPGMM.class);

    public StochVarDPGMM(MacroBaseConf conf) {
        super(conf);
    }
    @Override
    public void train(List<Datum> data) {
        // 0. Initialize all approximating factors
        log.debug("training locally");
        clusters = new NormalWishartClusters(T, data.get(0).getMetrics().getDimension());
        clusters.initializeBaseForDP(data);
        clusters.initializeAtomsForDP(data, conf.getRandom());

        log.debug("actual training");
        VariationalInference.trainStochastic(this, data, mixingComponents, clusters, 3500, 0.01, 0.3);
    }
}
