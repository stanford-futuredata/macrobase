package macrobase.analysis.periodic;

import com.codahale.metrics.Timer;
import macrobase.MacroBase;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.sample.ExponentiallyBiasedAChao;
import macrobase.analysis.summary.itemset.ExponentiallyDecayingEmergingItemsets;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by pbailis on 12/24/15.
 */
class RetrainingProcedure {
    private static final Logger log = LoggerFactory.getLogger(RetrainingProcedure.class);

    private static final Timer trainingTimer = MacroBase.metrics.timer(name(RetrainingProcedure.class, "modelTrainingTime"));

    public static void updatePeriod(ExponentiallyBiasedAChao<Datum> inputReservoir,
                                    OutlierDetector detector,
                                    ExponentiallyDecayingEmergingItemsets itemsets) {
        log.trace("Updating models.");

        Timer.Context rt = trainingTimer.time();
        detector.train(inputReservoir.getReservoir());
        rt.stop();
    }
}
