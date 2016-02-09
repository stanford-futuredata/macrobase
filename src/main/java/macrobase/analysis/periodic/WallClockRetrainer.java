package macrobase.analysis.periodic;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.sample.ExponentiallyBiasedAChao;
import macrobase.analysis.summary.itemset.ExponentiallyDecayingEmergingItemsets;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WallClockRetrainer extends AbstractWallClockPeriodicUpdater {
    private static final Logger log = LoggerFactory.getLogger(WallClockRetrainer.class);

    final ExponentiallyBiasedAChao<Datum> inputReservoir;
    final OutlierDetector detector;
    final ExponentiallyDecayingEmergingItemsets itemsets;

    public WallClockRetrainer(long startTime,
                              long periodMs,
                              ExponentiallyBiasedAChao<Datum> inputReservoir,
                              OutlierDetector detector,
                              ExponentiallyDecayingEmergingItemsets itemsets) {
        super(startTime, periodMs);
        this.inputReservoir = inputReservoir;
        this.detector = detector;
        this.itemsets = itemsets;
    }

    @Override
    void updatePeriod(Object additionalData) {
        log.trace("Retraining detectors due to period: {} tuples processed", getCurrentPeriodTime());

        RetrainingProcedure.updatePeriod(inputReservoir,
                                         detector,
                                         itemsets,
                                         additionalData);
    }
}
