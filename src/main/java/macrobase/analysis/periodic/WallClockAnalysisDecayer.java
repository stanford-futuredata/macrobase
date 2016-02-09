package macrobase.analysis.periodic;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.sample.ExponentiallyBiasedAChao;
import macrobase.analysis.summary.itemset.ExponentiallyDecayingEmergingItemsets;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WallClockAnalysisDecayer extends AbstractWallClockPeriodicUpdater {
    private static final Logger log = LoggerFactory.getLogger(WallClockAnalysisDecayer.class);

    ExponentiallyBiasedAChao<Datum> inputReservoir;
    ExponentiallyBiasedAChao<Double> scoreReservoir;
    OutlierDetector detector;
    ExponentiallyDecayingEmergingItemsets itemsets;

    public WallClockAnalysisDecayer(long startTime,
                                    long periodMs,
                                    ExponentiallyBiasedAChao<Datum> inputReservoir,
                                    ExponentiallyBiasedAChao<Double> scoreReservoir,
                                    OutlierDetector detector,
                                    ExponentiallyDecayingEmergingItemsets itemsets) {
        super(startTime, periodMs);
        this.inputReservoir = inputReservoir;
        this.scoreReservoir = scoreReservoir;
        this.detector = detector;
        this.itemsets = itemsets;
    }

    @Override
    void updatePeriod(Object additionalData) {
        log.trace("Decaying summarizers due to period: {} tuples processed", getCurrentPeriodTime());

        AnalysisDecayProcedure.updatePeriod(inputReservoir,
                                            scoreReservoir,
                                            detector,
                                            itemsets);
    }
}
