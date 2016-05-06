package macrobase.analysis.stats.mixture;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StochVarFiniteGMM extends FiniteGMM {
    private static final Logger log = LoggerFactory.getLogger(StochVarFiniteGMM.class);

    public StochVarFiniteGMM(MacroBaseConf conf) {
        super(conf);
    }

    @Override
    public void train(List<Datum> data) {
        // 0. Initialize all approximating factors
        log.debug("training locally");
        mixingComponents = new MultiComponents(0.1, K);
        clusters = new NormalWishartClusters(K, data.get(0).getMetrics().getDimension());
        clusters.initializeBaseForFinite(data);
        clusters.initalizeAtomsForFinite(data, initialClusterCentersFile, conf.getRandom());
        //clusters.initializeBase(baseLoc, baseBeta, baseOmega, baseNu);

        log.debug("actual training");
        VariationalInference.trainStochastic(this, data, mixingComponents, clusters, 7000, 0.001, 0.5);
    }
}
