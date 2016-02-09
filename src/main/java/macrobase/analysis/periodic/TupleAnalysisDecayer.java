package macrobase.analysis.periodic;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.sample.ExponentiallyBiasedAChao;
import macrobase.analysis.summary.itemset.ExponentiallyDecayingEmergingItemsets;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleAnalysisDecayer extends AbstractTupleBasedPeriodicUpdater {
    private static final Logger log = LoggerFactory.getLogger(TupleAnalysisDecayer.class);

    ExponentiallyBiasedAChao<Datum> inputReservoir;
    ExponentiallyBiasedAChao<Double> scoreReservoir;
    OutlierDetector detector;
    ExponentiallyDecayingEmergingItemsets itemsets;

    public TupleAnalysisDecayer(long tuplesPerPeriod,
                                ExponentiallyBiasedAChao<Datum> inputReservoir,
                                ExponentiallyBiasedAChao<Double> scoreReservoir,
                                OutlierDetector detector,
                                ExponentiallyDecayingEmergingItemsets itemsets) {
        super(tuplesPerPeriod);
        this.inputReservoir = inputReservoir;
        this.scoreReservoir = scoreReservoir;
        this.detector = detector;
        this.itemsets = itemsets;
    }

    @Override
    void updatePeriod(Object additionalData) {
        log.trace("Decaying summarizers due to period: {} tuples processed", getCurrentTupleCount());

        AnalysisDecayProcedure.updatePeriod(inputReservoir,
                                            scoreReservoir,
                                            detector,
                                            itemsets);
    }
}
