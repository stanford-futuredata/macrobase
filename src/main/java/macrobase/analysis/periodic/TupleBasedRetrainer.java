package macrobase.analysis.periodic;

import macrobase.analysis.outlier.OutlierDetector;
import macrobase.analysis.sample.ExponentiallyBiasedAChao;
import macrobase.analysis.summary.itemset.ExponentiallyDecayingEmergingItemsets;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pbailis on 12/24/15.
 */
public class TupleBasedRetrainer extends AbstractTupleBasedPeriodicUpdater {
    private static final Logger log = LoggerFactory.getLogger(TupleBasedRetrainer.class);

    final ExponentiallyBiasedAChao<Datum> inputReservoir;
    final OutlierDetector detector;


    public TupleBasedRetrainer(long tuplesPerPeriod,
                               ExponentiallyBiasedAChao<Datum> inputReservoir,
                               OutlierDetector detector) {
        super(tuplesPerPeriod);
        this.inputReservoir = inputReservoir;
        this.detector = detector;
    }

    @Override
    void updatePeriod() {
        log.trace("Retraining models due to period: {} tuples processed", getCurrentTupleCount());

        RetrainingProcedure.updatePeriod(inputReservoir,
                                         detector);
    }
}
