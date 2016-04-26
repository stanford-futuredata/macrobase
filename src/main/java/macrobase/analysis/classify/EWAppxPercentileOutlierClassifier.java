package macrobase.analysis.classify;

import macrobase.analysis.result.DatumWithNorm;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.sample.ExponentiallyBiasedAChao;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.Periodic;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/*
 Exponentially weighted approximate percentile-based streaming classifier
 */
public class EWAppxPercentileOutlierClassifier implements OutlierClassifier {
    protected Iterator<Datum> input;
    private ExponentiallyBiasedAChao<DatumWithNorm> reservoir;

    private double currentThreshold = 0;

    private final Periodic reservoirDecayer;
    private final Periodic percentileUpdater;

    List<OutlierClassificationResult> warmupOutput = new ArrayList<>();

    public EWAppxPercentileOutlierClassifier(MacroBaseConf conf, Iterator<Datum> input) {
        this(conf,
             input,
             conf.getInt(MacroBaseConf.SCORE_RESERVOIR_SIZE, MacroBaseDefaults.SCORE_RESERVOIR_SIZE),
             conf.getInt(MacroBaseConf.WARMUP_COUNT, MacroBaseDefaults.WARMUP_COUNT),
             conf.getDecayType(),
             conf.getDouble(MacroBaseConf.SUMMARY_UPDATE_PERIOD, MacroBaseDefaults.SUMMARY_UPDATE_PERIOD),
             conf.getDecayType(),
             conf.getDouble(MacroBaseConf.SUMMARY_UPDATE_PERIOD, MacroBaseDefaults.SUMMARY_UPDATE_PERIOD),
             conf.getDouble(MacroBaseConf.DECAY_RATE, MacroBaseDefaults.DECAY_RATE),
             conf.getDouble(MacroBaseConf.TARGET_PERCENTILE, MacroBaseDefaults.TARGET_PERCENTILE));
    }

    private void updateThreshold(double percentile) {
        List<DatumWithNorm> norms = reservoir.getReservoir();
        norms.sort((a, b) -> a.getNorm().compareTo(b.getNorm()));
        currentThreshold = norms.get((int)(percentile * norms.size())).getNorm();
    }

    public EWAppxPercentileOutlierClassifier(MacroBaseConf conf,
                                             Iterator<Datum> input,
                                             int sampleSize,
                                             int warmupCount,
                                             MacroBaseConf.PeriodType updatePeriodType,
                                             double updatePeriod,
                                             MacroBaseConf.PeriodType decayPeriodType,
                                             double decayPeriod,
                                             double decayRate,
                                             double percentile) {
        this.input = input;
        reservoir = new ExponentiallyBiasedAChao<>(sampleSize, decayRate, conf.getRandom());

        this.percentileUpdater = new Periodic(updatePeriodType,
                                              updatePeriod,
                                              () -> updateThreshold(percentile));

        this.reservoirDecayer = new Periodic(decayPeriodType,
                                             decayPeriod,
                                             reservoir::advancePeriod);

        List<DatumWithNorm> warmupInput = new ArrayList<>(warmupCount);
        for(int i = 0; i < warmupCount; ++i) {
            Datum d = input.next();
            DatumWithNorm dwn = new DatumWithNorm(d, d.getMetrics().getNorm());
            warmupInput.add(dwn);
            reservoir.insert(dwn);
            reservoirDecayer.runIfNecessary();
            percentileUpdater.runIfNecessary();
        }
        updateThreshold(percentile);

        for(DatumWithNorm dwn : warmupInput) {
            warmupOutput.add(new OutlierClassificationResult(dwn.getDatum(), dwn.getNorm() > currentThreshold));
        }
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public OutlierClassificationResult next() {
        if(!warmupOutput.isEmpty()) {
            return warmupOutput.remove(0);
        }

        reservoirDecayer.runIfNecessary();
        percentileUpdater.runIfNecessary();

        Datum d = input.next();
        double norm = d.getMetrics().getNorm();
        reservoir.insert(new DatumWithNorm(d, norm));

        return new OutlierClassificationResult(d, norm > currentThreshold);
    }
}
