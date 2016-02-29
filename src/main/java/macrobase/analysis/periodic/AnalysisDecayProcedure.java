package macrobase.analysis.periodic;

import macrobase.MacroBase;
import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.sample.ExponentiallyBiasedAChao;
import macrobase.analysis.summary.itemset.ExponentiallyDecayingEmergingItemsets;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by pbailis on 12/24/15.
 */
class AnalysisDecayProcedure {
    private static final Logger log = LoggerFactory.getLogger(AnalysisDecayProcedure.class);

    private static final Timer reservoirTimer = MacroBase.metrics.timer(
            name(AnalysisDecayProcedure.class, "reservoirAdvanceTime"));
    private static final Timer itemsetTimer = MacroBase.metrics.timer(
            name(AnalysisDecayProcedure.class, "itemsetAdvanceTime"));

    public static void updatePeriod(ExponentiallyBiasedAChao<Datum> inputReservoir,
                                    ExponentiallyBiasedAChao<Double> scoreReservoir,
                                    OutlierDetector detector,
                                    ExponentiallyDecayingEmergingItemsets itemsets) {
        log.trace("Updating analysis.");

        Timer.Context rt = reservoirTimer.time();
        inputReservoir.advancePeriod();

        if (scoreReservoir != null) {
            scoreReservoir.advancePeriod();
        }
        rt.stop();


        Timer.Context it = itemsetTimer.time();
        itemsets.markPeriod();
        it.stop();
    }
}
