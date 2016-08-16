package macrobase.analysis.classify;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.sample.FlexibleDampedReservoir;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.util.Periodic;

import java.util.ArrayList;
import java.util.List;

/*
 Exponentially weighted approximate percentile-based streaming classifier
 */

public class EWAppxPercentileOutlierClassifier extends OutlierClassifier {
    private final double percentile;
    private FlexibleDampedReservoir<DatumWithNorm> reservoir;

    private double currentThreshold = 0;

    private final Periodic reservoirDecayer;
    private final Periodic percentileUpdater;


    private final int warmupCount;
    private int tupleCount = 0;
    private final List<Datum> warmupInput = new ArrayList<>();

    private MBStream<OutlierClassificationResult> output = new MBStream<>();

    public EWAppxPercentileOutlierClassifier(MacroBaseConf conf) {
        this(conf,
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
                                             int sampleSize,
                                             int warmupCount,
                                             MacroBaseConf.PeriodType updatePeriodType,
                                             double updatePeriod,
                                             MacroBaseConf.PeriodType decayPeriodType,
                                             double decayPeriod,
                                             double decayRate,
                                             double percentile) {
        reservoir = new FlexibleDampedReservoir<>(sampleSize, decayRate, conf.getRandom());

        this.percentileUpdater = new Periodic(updatePeriodType,
                                              updatePeriod,
                                              () -> updateThreshold(percentile));

        this.reservoirDecayer = new Periodic(decayPeriodType,
                                             decayPeriod,
                                             reservoir::advancePeriod);

        this.warmupCount = warmupCount;
        this.percentile = percentile;
    }

    @Override
    public MBStream<OutlierClassificationResult> getStream() {
        return output;
    }

    @Override
    public void initialize() {

    }

    @Override
    public void consume(List<Datum> records) {
        List<OutlierClassificationResult> batchResult = new ArrayList<>(records.size());
        for(Datum d : records) {
            tupleCount ++;

            if(tupleCount < warmupCount) {
                warmupInput.add(d);
                DatumWithNorm dwn = new DatumWithNorm(d, d.metrics().getNorm());
                reservoir.insert(dwn);
                reservoirDecayer.runIfNecessary();
                percentileUpdater.runIfNecessary();
            } else {
                if(tupleCount == warmupCount) {
                    updateThreshold(percentile);

                    for(Datum di: warmupInput) {
                        batchResult.add(new OutlierClassificationResult(di, d.metrics().getNorm() > currentThreshold));
                    }
                    warmupInput.clear();
                }

                double norm = d.metrics().getNorm();
                reservoir.insert(new DatumWithNorm(d, norm));
                batchResult.add(new OutlierClassificationResult(d, norm > currentThreshold));
            }
        }

        output.add(batchResult);
    }

    @Override
    public void shutdown() {

    }
}
